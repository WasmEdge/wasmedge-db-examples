//! Routing nodes to determine responsible KVS nodes for given keys.

use crate::{
    config::Config,
    hash_ring::{GlobalHashRing, HashRingUtil, KeyReplication, LocalHashRing},
    messages::{self, Tier},
    metadata::TierMetadata,
    nodes::receive_tcp_message,
    topics::{KvsThread, MonitoringThread, RoutingThread},
    ClientKey, ZenohValueAsString,
};
use eyre::Context;
use futures::{future::FusedFuture, stream::FuturesUnordered, Future, FutureExt, StreamExt};
use smol::{
    channel,
    net::{TcpListener, TcpStream},
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    mem,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use zenoh::prelude::{Sample, ZFuture};

mod handlers;

/// Runs a new routing node based on the supplied config.
///
/// Spawns `config.threads.routing` concurrent tasks. For
/// communication with other nodes, the given `zenoh` workspace is used.
pub fn run(
    config: &Config,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    public_ip: Option<IpAddr>,
) -> eyre::Result<()> {
    let routing_thread_count = config.threads.routing;
    let node_id = format!("routing-{}", uuid::Uuid::new_v4());
    log::info!("Node {} starting up..", node_id);

    let config_data = Arc::new(ConfigData {
        tier_metadata: {
            let mut map = HashMap::new();
            map.insert(
                Tier::Memory,
                TierMetadata {
                    thread_number: config.threads.memory,
                    default_replication: config.replication.memory,
                    node_capacity: config.capacities.memory_cap * 1000000,
                },
            );
            map.insert(
                Tier::Disk,
                TierMetadata {
                    thread_number: config.threads.ebs,
                    default_replication: config.replication.ebs,
                    node_capacity: config.capacities.ebs_cap * 1000000,
                },
            );
            map
        },
        routing_thread_count,
        default_local_replication: config.replication.local,
        public_ip,
    });

    // start the initial worker threads based on `thread_num`
    //
    // We need to use scoped threads because the zenoh workspace does not have a 'static lifetime.
    crossbeam_utils::thread::scope(|s| {
        let (shutdown_tx, shutdown) = smol::channel::unbounded::<()>();
        let (result_tx, task_errors) = smol::channel::unbounded();

        for thread_id in 0..routing_thread_count {
            let config_data = config_data.clone();
            let node_id = node_id.clone();
            let result_tx = result_tx.clone();
            let mut shutdown = shutdown.clone();
            let zenoh = zenoh.clone();
            let zenoh_prefix = zenoh_prefix.clone();

            let task = async move {
                let mut router =
                    RoutingNode::new(node_id, thread_id, config_data, zenoh, zenoh_prefix);
                router
                    .run(shutdown.next().map(|_| ()))
                    .await
                    .context("routing thread failed")
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

/// Configuration options relevant to a routing node.
#[derive(Clone)]
struct ConfigData {
    tier_metadata: HashMap<Tier, TierMetadata>,
    routing_thread_count: u32,
    default_local_replication: usize,
    public_ip: Option<IpAddr>,
}

/// Routing nodes are responsible for replying to
/// [`AddressRequest`][crate::messages::AddressRequest] messages.
pub struct RoutingNode {
    thread_id: u32,
    node_id: String,
    config_data: Arc<ConfigData>,
    rt: RoutingThread,

    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,

    global_hash_rings: HashMap<Tier, GlobalHashRing>,
    local_hash_rings: HashMap<Tier, LocalHashRing>,
    key_replication_map: HashMap<ClientKey, KeyReplication>,
    pending_requests: HashMap<ClientKey, Vec<PendingRequest>>,
    hash_ring_util: HashRingUtil,

    node_sockets: HashMap<KvsThread, SocketAddr>,
    node_connections: HashMap<KvsThread, TcpStream>,

    known_routing_nodes: HashSet<String>,
}

impl RoutingNode {
    /// Creates a new routing node thread.
    fn new(
        node_id: String,
        thread_id: u32,
        config_data: Arc<ConfigData>,
        zenoh: Arc<zenoh::Session>,
        zenoh_prefix: String,
    ) -> Self {
        Self {
            thread_id,
            rt: RoutingThread::new(node_id.clone(), thread_id),
            node_id: node_id.clone(),
            zenoh,
            zenoh_prefix,
            global_hash_rings: Default::default(),
            local_hash_rings: Default::default(),
            key_replication_map: Default::default(),
            pending_requests: Default::default(),
            hash_ring_util: HashRingUtil::new(config_data.default_local_replication),
            config_data,
            node_sockets: Default::default(),
            node_connections: Default::default(),
            known_routing_nodes: [node_id].into_iter().collect(),
        }
    }

    /// Runs the routing node thread.
    async fn run(
        &mut self,
        mut shutdown_signal: impl Future<Output = ()> + FusedFuture + Unpin,
    ) -> eyre::Result<()> {
        if self.thread_id == 0 {
            // notify monitoring nodes
            // add null because it expects two IPs from server nodes...
            let message = messages::Notify::Join(messages::Join {
                tier: Tier::Routing,
                node_id: self.node_id.clone(),
                join_count: 0,
            });

            self.zenoh
                .put(
                    &MonitoringThread::notify_topic(&self.zenoh_prefix),
                    serde_json::to_string(&message)
                        .context("failed to serialize notify message")?,
                )
                .await
                .map_err(|e| eyre::eyre!(e))
                .context("failed to send join to monitoring nodes")?;
        }

        let advertisement_task = {
            let node_id = self.node_id.clone();
            let thread_id = self.thread_id;
            let zenoh = self.zenoh.clone();
            let zenoh_prefix = self.zenoh_prefix.clone();

            async move {
                if thread_id == 0 {
                    // advertise node to other routing nodes
                    let message = messages::RoutingNodeAdvertisement {
                        node_id: node_id.clone(),
                    };
                    loop {
                        zenoh
                            .put(
                                &RoutingThread::advertisement_topic(&zenoh_prefix),
                                serde_json::to_string(&message)
                                    .context("failed to serialize notify message")?,
                            )
                            .await
                            .map_err(|e| eyre::eyre!(e))
                            .context("failed to send advertisement message")?;
                        futures_timer::Delay::new(Duration::from_secs(10)).await;
                    }
                } else {
                    Result::<_, eyre::Error>::Ok(())
                }
            }
            .fuse()
        };
        futures::pin_mut!(advertisement_task);

        for (&tier, tier_meta) in self.config_data.tier_metadata.iter() {
            for thread_id in 0..tier_meta.thread_number {
                self.local_hash_rings
                    .entry(tier)
                    .or_default()
                    .insert_thread(thread_id);
            }
        }

        // subscribe to zenoh topics
        let zenoh = self.zenoh.clone();

        let mut tcp_addr_queryable = zenoh
            .queryable(&self.rt.tcp_addr_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare tcp_port queryable")?;
        let mut tcp_addr_request_stream = tcp_addr_queryable.receiver().fuse();

        // responsible for sending existing server addresses to a new node (relevant
        // to seed node)
        let mut address_queryable = zenoh
            .queryable(&RoutingThread::seed_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare address queryable")?;
        let mut address_request_stream = address_queryable.receiver().fuse();

        // responsible for both node join and departure
        let mut notify_subscriber = zenoh
            .subscribe(&self.rt.notify_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare notify subscriber")?;
        let mut notify_stream = notify_subscriber.receiver().fuse();

        // responsible for listening for key replication factor response
        let mut replication_response_subscriber = zenoh
            .subscribe(&self.rt.replication_response_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare replication response subscriber")?;
        let mut replication_response_stream = replication_response_subscriber.receiver().fuse();

        // responsible for handling key replication factor change requests from server
        // nodes
        let mut replication_change_subscriber = zenoh
            .subscribe(&self.rt.replication_change_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare replication change subscriber")?;
        let mut replication_change_stream = replication_change_subscriber.receiver().fuse();

        // responsible for handling key address request from users
        let mut key_address_subscriber = zenoh
            .subscribe(&self.rt.address_request_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare key address subscriber")?;
        let mut key_address_stream = key_address_subscriber.receiver().fuse();

        let mut routing_ad_subscriber = zenoh
            .subscribe(&RoutingThread::advertisement_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare advertisement subscriber")?;
        let mut routing_ad_stream = routing_ad_subscriber.receiver().fuse();

        // create TCP listener
        let tcp_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to create TCP listener")?;
        let tcp_port = tcp_listener
            .local_addr()
            .context("failed to get port number of TCP listener")?
            .port();
        let tcp_addr = self
            .config_data
            .public_ip
            .map(|ip| SocketAddr::new(ip, tcp_port));
        let tcp_addr_serialized =
            serde_json::to_string(&tcp_addr).context("failed to serialize tcp address")?;
        let mut tcp_incoming = tcp_listener.incoming().fuse();

        let (new_connections_tx, mut new_connections) = channel::bounded(10);
        let mut new_nodes_tasks = FuturesUnordered::new();

        let (new_messages_tx, mut new_messages) = channel::bounded(10);
        let mut new_messages_tasks = FuturesUnordered::new();

        let rt = self.rt.clone();
        let zenoh = self.zenoh.clone();
        let zenoh_prefix = self.zenoh_prefix.clone();
        std::thread::spawn(move || {
            let mut ping_subscriber = zenoh
                .subscribe(&rt.ping_topic(&zenoh_prefix))
                .wait()
                .map_err(|e| eyre::eyre!(e))
                .context("failed to declare key address subscriber")?;

            for sample in ping_subscriber.receiver().iter() {
                zenoh
                    .put(sample.value.as_string()?, "pong")
                    .wait()
                    .map_err(|e| eyre::eyre!(e))?;
            }
            Result::<_, eyre::Error>::Ok(())
        });

        let rt = self.rt.clone();
        let zenoh = self.zenoh.clone();
        let zenoh_prefix = self.zenoh_prefix.clone();
        std::thread::spawn(move || {
            let mut ping_subscriber = zenoh
                .queryable(&rt.ping_topic(&zenoh_prefix))
                .wait()
                .map_err(|e| eyre::eyre!(e))
                .context("failed to declare key address subscriber")?;

            smol::block_on(async {
                while let Some(request) = ping_subscriber.receiver().next().await {
                    request.reply_async(Sample::new("pong", "pong")).await;
                }
                Result::<_, eyre::Error>::Ok(())
            })
        });

        loop {
            futures::select! {
                listen_result = tcp_incoming.select_next_some() => {
                    let mut tcp_stream = listen_result
                        .context("failed to listen for new TCP connection")?;
                    tcp_stream.set_nodelay(true)
                        .context("failed to set nodelay for client tcp stream")?;
                    new_nodes_tasks.push(async {
                        let message = receive_tcp_message(&mut tcp_stream).await?;
                        if let Some(message) = message {
                            new_connections_tx.send((tcp_stream, message)).await
                                .context("failed to send new connection over channel")?;
                        }
                        Result::<_, eyre::Error>::Ok(())
                    });
                }
                task_result = new_nodes_tasks.select_next_some() => {
                    task_result?;
                }
                new_connection = new_connections.select_next_some() => {
                    let (mut tcp_stream, message) = new_connection;

                    self.handle_message(message, Cow::Borrowed(&tcp_stream)).await.context("failed to handle initial message")?;

                    // listen for more messages
                    let new_messages_tx = new_messages_tx.clone();
                    new_messages_tasks.push(async move {
                        loop {
                            let message = receive_tcp_message(&mut tcp_stream).await?;
                            match message {
                                Some(message) => {
                                    new_messages_tx.send((message, tcp_stream.clone())).await.context(
                                        "failed to send incoming message over channel"
                                    )?;
                                }
                                None => break Result::<_, eyre::Error>::Ok(()),
                            }
                        }
                    })
                }
                task_result = new_messages_tasks.select_next_some() => {
                    task_result?;
                }
                message = new_messages.select_next_some() => {
                    let (message, tcp_stream) = message;
                    self.handle_message(message, Cow::Owned(tcp_stream)).await
                        .context("failed to handle message")?;
                }

                query = tcp_addr_request_stream.select_next_some() => {
                    query.reply_async(Sample::new(query.key_selector().to_owned(), tcp_addr_serialized.as_str())).await;
                }
                query = address_request_stream.select_next_some() => {
                    log::info!("handling address request for {}", query.selector());
                    let serialized_reply = self.seed_handler();
                    query.reply_async(Sample::new(query.key_selector().to_owned() ,serialized_reply)).await;
                }
                sample = notify_stream.select_next_some() => {
                    let parsed = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize Notify message")?;
                    self.membership_handler(&parsed).await.context("membership handler failed")?;
                }
                sample = replication_response_stream.select_next_some() => {
                    log::info!("received replication response");
                    let parsed = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize Response message")?;
                    self.replication_response_handler(parsed).await
                        .context("replication response handler failed")?;
                }
                sample = replication_change_stream.select_next_some() => {
                    self.replication_change_handler(&sample.value.as_string()?).await
                        .context("replication change handler failed")?;
                }
                sample = key_address_stream.select_next_some() => {
                    let parsed = serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize AddressRequest message")?;
                    self.address_handler(parsed, None).await
                        .context("address handler failed")?;
                },
                sample = routing_ad_stream.select_next_some() => {
                    let messages::RoutingNodeAdvertisement { node_id } =
                        serde_json::from_str(&sample.value.as_string()?)
                        .context("failed to deserialize RoutingNodeAdvertisement message")?;
                    self.known_routing_nodes.insert(node_id);
                },
                () = shutdown_signal => break,
                result = advertisement_task => result?,
                complete => break,
            }
        }

        log::info!("routing task is finished");

        Ok(())
    }
}

struct PendingRequest {
    request_id: String,
    reply_path: String,
    reply_stream: Option<TcpStream>,
}

#[cfg(test)]
fn router_test_instance(zenoh: Arc<zenoh::Session>, zenoh_prefix: String) -> RoutingNode {
    let config_data = ConfigData {
        routing_thread_count: 1,
        default_local_replication: 1,
        tier_metadata: Default::default(),
        public_ip: None,
    };
    let node_id: String = "router_id".into();
    let mut router = RoutingNode::new(
        node_id.clone(),
        0,
        Arc::new(config_data),
        zenoh,
        zenoh_prefix,
    );

    router
        .global_hash_rings
        .entry(Tier::Memory)
        .or_default()
        .insert_node(node_id, 0);
    router
        .local_hash_rings
        .entry(Tier::Memory)
        .or_default()
        .insert_thread(0);

    router
}
