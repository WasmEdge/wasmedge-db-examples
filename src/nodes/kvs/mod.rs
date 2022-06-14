//! The main key-value store nodes.

pub use self::gossip::{GOSSIP_PERIOD, REPORT_PERIOD};
use self::report::ReportData;
use crate::{
    config::Config,
    hash_ring::{tier_name, GlobalHashRing, HashRingUtil, KeyReplication, LocalHashRing},
    messages::{
        self, cluster_membership::ClusterInfo, response::ResponseType, Response, TcpMessage, Tier,
    },
    metadata::TierMetadata,
    store::{LatticeValue, LatticeValueStore},
    topics::{KvsThread, MonitoringThread, RoutingThread},
    ClientKey, Key, ZenohValueAsString,
};
use eyre::{bail, eyre, Context};
use futures::{
    future::FusedFuture,
    stream::{self, FusedStream, FuturesUnordered, SelectAll},
    Future, FutureExt, Stream, StreamExt, TryStreamExt,
};
use smol::net::{TcpListener, TcpStream};
use std::{
    collections::{HashMap, HashSet},
    mem,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use super::{receive_tcp_message, request_cluster_info, send_tcp_message};

mod gossip;
mod handlers;
mod report;

/// Starts a new multithreaded KVS node based on the given config.
pub fn run(
    config: &Config,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    public_ip: Option<IpAddr>,
) -> eyre::Result<()> {
    // populate metadata
    let self_tier = match std::env::var("SERVER_TYPE").as_deref() {
        Ok("memory") => Tier::Memory,
        Ok("ebs") => Tier::Disk,
        Ok(other) => bail!(
            "Unrecognized server type `{}`. Valid types are `memory` and `ebs`.",
            other
        ),
        Err(std::env::VarError::NotPresent) => {
            log::info!(
                "No server type specified. The default behavior is to start the \
                server in memory mode."
            );
            Tier::Memory
        }
        Err(std::env::VarError::NotUnicode(_)) => bail!("SERVER_TYPE is not valid unicode"),
    };

    let mut tier_metadata = HashMap::new();
    tier_metadata.insert(
        Tier::Memory,
        TierMetadata {
            thread_number: config.threads.memory,
            default_replication: config.replication.memory,
            node_capacity: config.capacities.memory_cap * 1000000,
        },
    );
    tier_metadata.insert(
        Tier::Disk,
        TierMetadata {
            thread_number: config.threads.ebs,
            default_replication: config.replication.ebs,
            node_capacity: config.capacities.ebs_cap * 1000000,
        },
    );

    let thread_num = tier_metadata[&self_tier].thread_number;

    let node_id = format!("kvs-{}", uuid::Uuid::new_v4());
    log::info!("Node {} starting up..", node_id);

    let config_data = ConfigData {
        self_tier,
        tier_metadata,
        thread_num,
        default_local_replication: config.replication.local,
        routing_thread_num: config.threads.routing,
        public_ip,
    };

    // start the initial worker threads based on `thread_num`
    //
    // We need to use scoped threads because the zenoh workspace does not have a 'static lifetime.
    crossbeam_utils::thread::scope(|s| {
        let (shutdown_tx, shutdown) = smol::channel::unbounded::<()>();
        let (result_tx, task_errors) = smol::channel::unbounded();

        for thread_id in 0..thread_num {
            let mgmt_node = None; // TODO
            let config_data = config_data.clone();
            let zenoh = zenoh.clone();
            let zenoh_prefix = zenoh_prefix.clone();
            let node_id = node_id.clone();
            let result_tx = result_tx.clone();
            let mut shutdown = shutdown.clone();

            let task = async move {
                let node = KvsNode::init(
                    node_id.clone(),
                    thread_id,
                    mgmt_node,
                    config_data,
                    zenoh,
                    zenoh_prefix,
                )
                .await?;
                node.run(shutdown.next().map(|_| ()))
                    .await
                    .context(format!("KVS thread {}/{} failed", node_id, thread_id))
            };
            s.spawn(move |_| {
                smol::block_on(async {
                    match task.await {
                        Ok(()) => {}
                        Err(err) => {
                            let _ = result_tx.send(err).await;
                        }
                    }
                })
            });
        }

        mem::drop(result_tx);
        mem::drop(shutdown);

        smol::block_on(task_errors.recv().map(|recv_result| match recv_result {
            Ok(error) => {
                shutdown_tx.close();
                Err(error)
            }
            Err(smol::channel::RecvError) => Ok(()),
        }))
    })
    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;

    Ok(())
}

/// A key-value store node thread.
pub struct KvsNode {
    /// The node ID of the node thread.
    node_id: String,
    /// The thread ID of the node thread.
    thread_id: u32,

    /// A counter that is increased if a node is re-joining.
    self_join_count: u32,

    /// Used for addressing itself.
    ///
    /// Must be consistent with the `node_id` and `thread_id` fields.
    ///
    /// The `wt` name is short for "worker thread".
    wt: KvsThread,

    /// Information about the cluster and its KVS nodes.
    membership: ClusterInfo,

    /// The relevant parts of the config file.
    config_data: ConfigData,

    /// Used for communicating with other nodes.
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,

    /// For each tier, determines which nodes are responsible for each key.
    global_hash_rings: HashMap<Tier, GlobalHashRing>,
    /// For each tier, determines which worker threads within a node are responsible for each key.
    local_hash_rings: HashMap<Tier, LocalHashRing>,
    /// Provides utiliy functions to get the responsible threads.
    ///
    /// Based on the configured default replication factor.
    hash_ring_util: HashRingUtil,

    /// Key-value store that stores the lattice values that this node is responsible for.
    kvs: LatticeValueStore<Key>,

    /// Caches the replication factor for each key.
    key_replication_map: HashMap<ClientKey, KeyReplication>,

    /// Keeps track of the start time of the current gossip period.
    ///
    /// Set to the current time after gossip was sent out.
    gossip_start: Instant,

    /// A monotonically increasing integer used for creating request IDs.
    request_id: u32,

    // for periodically redistributing data when node joins
    join_gossip_map: HashMap<String, HashSet<Key>>,

    // keep track of which key should be removed when node joins
    join_remove_set: HashSet<Key>,

    // pending events for asynchrony
    pending_requests: HashMap<Key, Vec<PendingRequest>>,
    pending_gossip: HashMap<Key, Vec<PendingGossip>>,

    // the set of changes made on this thread since the last round of gossip
    local_changeset: HashSet<Key>,

    // For tracking the caches that hold a given key.
    // Inverse of cache_ip_to_keys.
    // We need the two structures because
    // key->caches is the one necessary for gossiping upon key updates,
    // but the mapping is provided to us in the form cache->keys,
    // so we need a local copy of this mapping in order to update key->caches
    // with dropped keys when we receive a fresh cache->keys record.
    key_to_cache_ids: HashMap<Key, HashSet<String>>,

    // For tracking the keys each extant cache is responsible for.
    // This is just our thread's cache of this.
    cache_id_to_keys: HashMap<String, HashSet<ClientKey>>,

    // for tracking IP addresses of extant caches
    extant_caches: HashSet<String>,

    /// The node collects some statistics and sends them out periodically.
    report_data: ReportData,

    node_connections: HashMap<KvsThread, TcpStream>,
}

impl KvsNode {
    /// Creates a new KVS node from the given parameters.
    pub async fn init(
        node_id: String,
        thread_id: u32,
        management_id: Option<String>,
        config_data: ConfigData,
        zenoh: Arc<zenoh::Session>,
        zenoh_prefix: String,
    ) -> eyre::Result<KvsNode> {
        let membership = request_cluster_info(&zenoh, &zenoh_prefix).await?;

        Ok(Self::new(
            node_id,
            thread_id,
            membership,
            management_id,
            config_data,
            zenoh,
            zenoh_prefix,
        ))
    }

    /// Creates a new KVS node in the given cluster.
    fn new(
        node_id: String,
        thread_id: u32,
        membership: ClusterInfo,
        management_id: Option<String>,
        config_data: ConfigData,
        zenoh: Arc<zenoh::Session>,
        zenoh_prefix: String,
    ) -> KvsNode {
        // get join number from management node if we are running in Kubernetes
        // (if we are running the system outside of Kubernetes, we need to set the
        // management address to NULL in the conf file, otherwise we will hang
        // forever waiting to hear back about a restart count)
        let self_join_count = if management_id.is_some() {
            // TODO: not fully ported to zenoh yet as it is only needed when running on
            // k8s
            // zmq::socket_t join_count_requester(context, ZMQ_REQ);
            // join_count_requester.connect(get_join_count_req_address(management_ip));
            // zenoh_publish(zenoh_session, get_join_count_req_address(management_ip),
            //               "restart:" + private_ip, log);
            // count_str = kZmqUtil->recv_string(&join_count_requester);
            todo!()
        } else {
            0
        };
        let mut node = Self {
            thread_id,
            node_id: node_id.clone(),
            self_join_count,
            wt: KvsThread::new(node_id.clone(), thread_id),
            membership,
            hash_ring_util: HashRingUtil::new(config_data.default_local_replication),
            config_data,
            zenoh,
            zenoh_prefix,
            gossip_start: Instant::now(),
            report_data: ReportData::new(management_id),
            request_id: 0,
            kvs: Default::default(),
            global_hash_rings: Default::default(),
            local_hash_rings: Default::default(),
            join_gossip_map: Default::default(),
            join_remove_set: Default::default(),
            pending_requests: Default::default(),
            pending_gossip: Default::default(),
            key_replication_map: Default::default(),
            local_changeset: Default::default(),
            key_to_cache_ids: Default::default(),
            cache_id_to_keys: Default::default(),
            extant_caches: Default::default(),
            node_connections: Default::default(),
        };
        for tier in &node.membership.tiers {
            let id = tier.tier_id;

            for server in &tier.servers {
                node.global_hash_rings
                    .entry(id)
                    .or_default()
                    .insert_node(server.clone(), 0);
            }
        }
        node.global_hash_rings
            .entry(node.config_data.self_tier)
            .or_default()
            .insert_node(node_id, node.self_join_count);
        for (&tier, tier_meta) in node.config_data.tier_metadata.iter() {
            for thread_id in 0..tier_meta.thread_number {
                node.local_hash_rings
                    .entry(tier)
                    .or_default()
                    .insert_thread(thread_id);
            }
        }

        node
    }

    /// Starts the KVS node.
    pub async fn run(
        mut self,
        mut shutdown_signal: impl Future<Output = ()> + FusedFuture + Unpin,
    ) -> eyre::Result<()> {
        // create TCP listener if a public IP is configured
        let tcp_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to create TCP listener")?;
        let tcp_listen_port = tcp_listener
            .local_addr()
            .context("failed to get port number of TCP listener")?
            .port();
        let tcp_listen_addr = self
            .config_data
            .public_ip
            .map(|ip| SocketAddr::new(ip, tcp_listen_port));

        let routing_tcp_streams = self
            .init_tcp_connections(tcp_listen_addr)
            .await
            .context("failed to init tcp connections")?;

        let mut tcp_incoming = tcp_listener.incoming().fuse();

        // thread 0 notifies other servers that it has joined
        if self.thread_id == 0 {
            let join_msg = messages::Join {
                tier: self.config_data.self_tier,
                node_id: self.node_id.clone(),
                join_count: self.self_join_count,
            };

            for hash_ring in self.global_hash_rings.values() {
                for node_id in hash_ring.unique_nodes() {
                    if node_id != self.node_id.as_str() {
                        self.zenoh
                            .put(
                                &KvsThread::new(node_id.clone(), 0)
                                    .node_join_topic(&self.zenoh_prefix),
                                serde_json::to_string(&join_msg)
                                    .context("failed to serialize join message")?,
                            )
                            .await
                            .map_err(|e| eyre!(e))
                            .context("failed to send join message to servers")?;
                    }
                }
            }

            let notify_msg = serde_json::to_string(&messages::Notify::Join(join_msg))
                .context("failed to serialize notify message")?;

            // notify proxies that this node has joined
            for node_id in &self.membership.routing_node_ids {
                self.zenoh
                    .put(
                        &RoutingThread::new(node_id.clone(), 0).notify_topic(&self.zenoh_prefix),
                        notify_msg.as_str(),
                    )
                    .await
                    .map_err(|e| eyre!(e))
                    .context("failed to send join message to routing nodes")?;
            }

            // notify monitoring nodes that this node has joined
            self.zenoh
                .put(
                    &MonitoringThread::notify_topic(&self.zenoh_prefix),
                    notify_msg.as_str(),
                )
                .await
                .map_err(|e| eyre!(e))
                .context("failed to send join message to monitoring nodes")?;
        }

        let zenoh = self.zenoh.clone();

        // listens for a new node joining
        let mut join_subscriber = zenoh
            .subscribe(&self.wt.node_join_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare join subscriber")?;
        let mut join_stream = join_subscriber.receiver().fuse();

        // listens for a node departing
        let mut depart_subscriber = zenoh
            .subscribe(&self.wt.node_depart_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare depart subscriber")?;
        let mut depart_stream = depart_subscriber.receiver().fuse();

        // responsible for listening for a command that this node should leave
        let mut self_depart_subscriber = zenoh
            .subscribe(&self.wt.self_depart_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare self depart subscriber")?;
        let mut self_depart_stream = self_depart_subscriber.receiver().fuse();

        // responsible for handling requests
        let mut request_subscriber = zenoh
            .subscribe(&self.wt.request_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare request subscriber")?;
        let mut request_stream = request_subscriber.receiver().fuse();

        // responsible for processing gossip
        let mut gossip_subscriber = zenoh
            .subscribe(&self.wt.gossip_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare gossip subscriber")?;
        let mut gossip_stream = gossip_subscriber.receiver().fuse();

        // responsible for listening for key replication factor response
        let mut replication_response_subscriber = zenoh
            .subscribe(&self.wt.replication_response_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare replication response subscriber")?;
        let mut replication_response_stream = replication_response_subscriber.receiver().fuse();

        // responsible for listening for key replication factor change
        let mut replication_change_subscriber = zenoh
            .subscribe(&self.wt.replication_change_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare replication change subscriber")?;
        let mut replication_change_stream = replication_change_subscriber.receiver().fuse();

        // responsible for listening for cached keys response messages.
        let mut cache_ip_subscriber = zenoh
            .subscribe(&self.wt.cache_ip_response_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare cache ip subscriber")?;
        let mut cache_ip_stream = cache_ip_subscriber.receiver().fuse();

        // responsible for listening for function node IP lookup response messages.
        let mut management_node_response_subscriber = zenoh
            .subscribe(&self.wt.management_node_response_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare management node response subscriber")?;
        let mut management_node_response_stream =
            management_node_response_subscriber.receiver().fuse();

        match self.config_data.self_tier {
            Tier::Memory => {
                // TODO: We currently hardcode the `self.kvs` field to `LatticeValueStore`,
                // which stores the data in memory. To support different tiers, we should
                // introduce a trait to make the field generic. For example, we should
                // serialize the data to disk for the disk tier.
            }
            other => bail!(
                "Storage for tier `{}` is not supported yet",
                tier_name(other)
            ),
        }

        let mut tcp_streams = SelectAll::new();
        let unfold_stream = |mut stream: TcpStream| {
            Box::pin(async {
                let message = receive_tcp_message(&mut stream).await?;
                Result::<_, eyre::Error>::Ok(message.map(|m| ((m, stream.clone()), stream)))
            })
        };
        for tcp_stream in routing_tcp_streams {
            tcp_streams.push(stream::try_unfold(tcp_stream, unfold_stream));
        }

        let self_connect = TcpStream::connect(tcp_listener.local_addr()?).fuse();
        smol::pin!(self_connect);

        loop {
            futures::select! {
                listen_result = tcp_incoming.select_next_some() => {
                    let tcp_stream = listen_result
                        .context("failed to listen for new TCP connection")?;
                    tcp_stream.set_nodelay(true)
                        .context("failed to set nodelay for client tcp stream")?;
                    tcp_streams.push(stream::try_unfold(tcp_stream, unfold_stream));
                }
                message = tcp_streams.select_next_some() => {
                    let (message, stream) = message.context("failed to receive message")?;
                    self.handle_message(message, stream).await.context("failed to handle TCP message")?;
                }
                result = self_connect => {
                    let self_connection = result?;
                    self_connection.set_nodelay(true)
                        .context("failed to set nodelay for client tcp stream")?;
                    tcp_streams.push(stream::try_unfold(self_connection.clone(), unfold_stream));
                    self.node_connections.insert(self.wt.clone(), self_connection);
                }
                sample = join_stream.select_next_some() => {
                    let message = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize join message")?;
                    self.node_join_handler(message).await.context("failed to handle join")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = depart_stream.select_next_some() => {
                    let message = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize join message")?;
                    self.node_depart_handler(message).await.context("failed to handle depart")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = self_depart_stream.select_next_some() => {
                    self.self_depart_handler(&sample.value.as_string()?)
                        .await.context("failed to handle self depart")?;
                },
                sample = request_stream.select_next_some() => {
                    let message = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize join message")?;
                    self.request_handler(message, None).await.context("failed to handle request")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = gossip_stream.select_next_some() =>  {
                    self.gossip_handler(&sample.value.as_string()?)
                        .await.context("failed to handle gossip")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = replication_response_stream.select_next_some() => {
                    let message = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize join message")?;
                    self.replication_response_handler(message)
                        .await.context("failed to handle replication_response")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = replication_change_stream.select_next_some() => {
                    self.replication_change_handler(&sample.value.as_string()?)
                        .await.context("failed to handle replication_change")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = cache_ip_stream.select_next_some() => {
                    self.cache_ip_handler(&sample.value.as_string()?)
                        .await.context("failed to handle cache_ip")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                sample = management_node_response_stream.select_next_some() => {
                    self.management_node_response_handler(&sample.value.as_string()?)
                        .await.context("failed to handle management_node_response")?;
                    self.gossip_updates().await.context("failed to gossip updates")?;
                },
                () = shutdown_signal => break,
                complete => break,
            }
        }

        Ok(())
    }

    async fn init_tcp_connections(
        &mut self,
        listen_socket: Option<SocketAddr>,
    ) -> eyre::Result<Vec<TcpStream>> {
        let tasks = FuturesUnordered::new();

        let routing_threads = self.membership.routing_node_ids.iter().flat_map(|id| {
            (0..self.config_data.routing_thread_num).map(move |i| RoutingThread::new(id.clone(), i))
        });
        for routing_thread in routing_threads {
            let s = &self;
            tasks.push(async move {
                let reply = loop {
                    let topic = routing_thread.tcp_addr_topic(&s.zenoh_prefix);
                    let mut receiver = s
                        .zenoh
                        .get(&topic)
                        .await
                        .map_err(|e| eyre!(e))
                        .context("failed to query tcp address of routing thread")?;
                    match receiver.next().await {
                        Some(reply) => break reply.sample.value,
                        None => {
                            log::info!("failed to receive tcp address reply");
                            futures_timer::Delay::new(Duration::from_secs(1)).await;
                        }
                    }
                };
                let parsed: Option<smol::net::SocketAddr> =
                    serde_json::from_str(&reply.as_string()?)
                        .context("failed to deserialize tcp addr reply")?;
                let connection = match parsed {
                    Some(addr) => {
                        let mut connection = TcpStream::connect(addr)
                            .await
                            .context("failed to connect to tcp stream")?;
                        connection
                            .set_nodelay(true)
                            .context("failed to set nodelay for tcpstream")?;
                        send_tcp_message(
                            &TcpMessage::TcpJoin {
                                kvs_thread: KvsThread {
                                    node_id: s.node_id.clone(),
                                    thread_id: s.thread_id,
                                },
                                listen_socket,
                            },
                            &mut connection,
                        )
                        .await
                        .context("failed to send TCP join message")?;
                        Some(connection)
                    }
                    None => None,
                };

                Result::<_, eyre::Error>::Ok(connection)
            });
        }

        let routing_thread_connections: Vec<TcpStream> = tasks
            .filter_map(|c| futures::future::ready(c.transpose()))
            .try_collect()
            .await?;

        Ok(routing_thread_connections)
    }

    async fn handle_message(
        &mut self,
        message: TcpMessage,
        tcp_stream: TcpStream,
    ) -> eyre::Result<()> {
        match message {
            TcpMessage::KvsSocket { kvs_thread, socket } => {
                let connection = TcpStream::connect(socket)
                    .await
                    .context("failed to connect to tcp stream")?;
                connection
                    .set_nodelay(true)
                    .context("failed to set nodelay for tcpstream")?;
                self.node_connections.insert(kvs_thread.clone(), connection);
                return Ok(()); // no gossip
            }
            TcpMessage::Notify(notify) => match notify {
                messages::Notify::Join(join) => self
                    .node_join_handler(join)
                    .await
                    .context("failed to handle join")?,
                messages::Notify::Depart(depart) => self
                    .node_depart_handler(depart)
                    .await
                    .context("failed to handle depart")?,
            },
            TcpMessage::Request(request) => self
                .request_handler(request, Some(tcp_stream))
                .await
                .context("failed to handle request")?,
            TcpMessage::Response(response) => self
                .replication_response_handler(response)
                .await
                .context("failed to handle response")?,
            other => bail!("unexpected message: {:?}", other),
        }
        self.gossip_updates()
            .await
            .context("failed to gossip updates")?;
        Ok(())
    }
}

trait TcpMessageStream: FusedStream + Stream<Item = eyre::Result<TcpMessage>> + Send + Sync {}

impl<T> TcpMessageStream for T where
    T: FusedStream + Stream<Item = eyre::Result<TcpMessage>> + Send + Sync
{
}

/// Configuration options for KVS nodes.
#[derive(Debug, Clone)]
pub struct ConfigData {
    /// The tier in which the node should run.
    pub self_tier: Tier,
    /// The number of threads that exist for this KVS node.
    pub thread_num: u32,
    /// The default replication factor for this tier.
    pub default_local_replication: usize,
    /// Metadata for all available tiers.
    pub tier_metadata: HashMap<Tier, TierMetadata>,
    /// The number of routing threads that exist per routing node.
    pub routing_thread_num: u32,
    /// IP address at which the node can be reached (optional).
    ///
    /// This address can be used by other nodes to create a direct TCP connection
    /// to instead of communicating through `zenoh`. This can reduce the latency
    /// significantly, at the cost of reduced flexibility.
    pub public_ip: Option<IpAddr>,
}

#[derive(Debug)]
struct PendingRequest {
    ty: ResponseType,
    lattice: Option<LatticeValue>,
    addr: Option<String>,
    reply_stream: Option<TcpStream>,
    response_id: Option<String>,
}

impl PendingRequest {
    fn new_response(&self) -> Response {
        Response {
            response_id: self.response_id.clone(),
            ty: self.ty,
            tuples: Default::default(),
            error: Ok(()),
        }
    }
}

struct PendingGossip {
    lattice_value: LatticeValue,
}

#[cfg(test)]
fn kvs_test_instance<'a>(zenoh: Arc<zenoh::Session>, zenoh_prefix: String) -> KvsNode {
    let config_data = ConfigData {
        self_tier: Tier::Memory,
        thread_num: 1,
        default_local_replication: 1,
        tier_metadata: Default::default(),
        routing_thread_num: 1,
        public_ip: None,
    };
    let node_id: String = "server_id".into();
    let mut server = KvsNode::new(
        node_id.clone(),
        0,
        // empty cluster
        ClusterInfo {
            tiers: Vec::new(),
            routing_node_ids: Vec::new(),
        },
        None,
        config_data,
        zenoh,
        zenoh_prefix,
    );
    server
        .global_hash_rings
        .entry(Tier::Memory)
        .or_default()
        .insert_node(node_id, 0);
    server
        .local_hash_rings
        .entry(Tier::Memory)
        .or_default()
        .insert_thread(0);

    server
}
