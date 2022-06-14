//! Monitoring nodes observe and react to key access patterns and node statistics.

use self::stats::SummaryStats;
use crate::{
    config::Config,
    hash_ring::{tier_name, GlobalHashRing, HashRingUtil, KeyReplication, LocalHashRing},
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice},
    messages::{
        management::AddNodes,
        replication_factor::{ReplicationFactor, ReplicationFactorUpdate, ReplicationValue},
        request::{PutTuple, RequestData},
        Request, Response, SelfDepart, Tier,
    },
    metadata::{MetadataKey, TierMetadata},
    store::LatticeValue,
    topics::{KvsThread, ManagementThread, MonitoringThread, RoutingThread},
    AnnaError, ClientKey, Key, ZenohValueAsString, ALL_TIERS,
};
use eyre::{anyhow, bail, eyre, Context};
use futures::StreamExt;
use rand::prelude::SliceRandom;
use std::{
    collections::{hash_map, HashMap, HashSet},
    convert::TryFrom,
    sync::Arc,
    time::{Duration, Instant},
};

const MONITORING_PERIOD: Duration = Duration::from_secs(30);
/// Defines the grace period for triggering actions to prevent over-correction.
const GRACE_PERIOD: Duration = Duration::from_secs(120);

// Default number of nodes to add concurrently for storage
const NODE_ADDITION_BATCH_SIZE: usize = 2;

// define capacity for both tiers
const MAX_MEMORY_NODE_CONSUMPTION: f64 = 0.6;
const MAX_EBS_NODE_CONSUMPTION: f64 = 0.75;
const MIN_EBS_NODE_CONSUMPTION: f64 = 0.5;

// define threshold for promotion/demotion
const KEY_PROMOTION_THRESHOLD: usize = 0;
const KEY_DEMOTION_THRESHOLD: usize = 1;

// define minimum number of nodes for each tier
const K_MIN_MEMORY_TIER_SIZE: usize = 1;
const MIN_EBS_TIER_SIZE: usize = 0;

const SLO_WORST: f64 = 3000.;

mod handlers;
mod policies;
mod stats;

/// Runs a new monitor node based on the supplied config.
pub fn run(config: &Config, zenoh: Arc<zenoh::Session>, zenoh_prefix: String) -> eyre::Result<()> {
    let config_data = ConfigData {
        memory_thread_count: config.threads.memory,
        ebs_thread_count: config.threads.ebs,
        default_local_replication: config.replication.local,
        minimum_replica_number: config.replication.minimum,
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
        enable_elasticity: config.policy.elasticity,
        enable_tiering: config.policy.tiering,
        enable_selective_rep: config.policy.selective_rep,
    };

    let node_id = format!("monitoring-{}", uuid::Uuid::new_v4());

    smol::block_on(MonitoringNode::run(
        node_id,
        config_data,
        zenoh,
        zenoh_prefix,
    ))?;

    Ok(())
}

/// Configuration options relevant to a monitor node.
#[derive(Clone)]
pub struct ConfigData {
    memory_thread_count: u32,
    ebs_thread_count: u32,
    tier_metadata: HashMap<Tier, TierMetadata>,
    default_local_replication: usize,
    enable_elasticity: bool,
    enable_tiering: bool,
    enable_selective_rep: bool,
    minimum_replica_number: usize,
}

/// Creates periodical statistics about running nodes and executes enabled policy mechanisms.
pub struct MonitoringNode<'a> {
    config_data: ConfigData,

    mt: MonitoringThread,
    management_node: Option<ManagementThread>,

    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    responses: zenoh::subscriber::Subscriber<'a>,

    departing_node_map: HashMap<String, u32>,
    grace_start: Instant,
    new_memory_count: usize,
    new_ebs_count: usize,
    routing_node_ids: HashSet<String>,

    key_access_frequency: HashMap<Key, HashMap<KvsThread, usize>>,
    key_access_summary: HashMap<Key, usize>,
    key_size: HashMap<ClientKey, usize>,

    memory_storage: HashMap<String, HashMap<u32, u64>>,
    memory_occupancy: HashMap<String, HashMap<u32, (f64, usize)>>,
    memory_accesses: HashMap<String, HashMap<u32, usize>>,
    ebs_storage: HashMap<String, HashMap<u32, u64>>,
    ebs_occupancy: HashMap<String, HashMap<u32, (f64, usize)>>,
    ebs_accesses: HashMap<String, HashMap<u32, usize>>,

    user_latency: HashMap<String, f64>,
    user_throughput: HashMap<String, f64>,
    latency_miss_ratio: HashMap<ClientKey, (f64, usize)>,

    global_hash_rings: HashMap<Tier, GlobalHashRing>,
    local_hash_rings: HashMap<Tier, LocalHashRing>,
    key_replication_map: HashMap<ClientKey, KeyReplication>,
    hash_ring_util: HashRingUtil,

    report_start: Instant,
    server_monitoring_epoch: usize,
    memory_node_count: usize,
    ebs_node_count: usize,

    summary_stats: SummaryStats,

    request_id: usize,

    removing_memory_node: bool,
    removing_ebs_node: bool,
}

impl MonitoringNode<'static> {
    /// Runs a new monitoring node with the given ID and configuration.
    pub async fn run(
        node_id: String,
        config_data: ConfigData,
        zenoh: Arc<zenoh::Session>,
        zenoh_prefix: String,
    ) -> eyre::Result<()> {
        let mt = MonitoringThread {
            node_id: node_id.clone(),
        };

        let zenoh_clone = zenoh.clone();
        let responses = zenoh_clone
            .subscribe(&mt.response_topic(&zenoh_prefix))
            .await
            .map_err(|e| eyre!(e))
            .context("failed to declare notify subscriber")?;

        let monitor = MonitoringNode {
            hash_ring_util: HashRingUtil::new(config_data.default_local_replication),

            mt,
            management_node: Some(ManagementThread::singleton()),

            zenoh,
            zenoh_prefix,
            config_data,
            responses,

            grace_start: Instant::now(),
            report_start: Instant::now(),

            departing_node_map: Default::default(),
            new_memory_count: Default::default(),
            new_ebs_count: Default::default(),
            routing_node_ids: Default::default(),
            key_access_frequency: Default::default(),
            key_access_summary: Default::default(),
            key_size: Default::default(),
            memory_storage: Default::default(),
            memory_occupancy: Default::default(),
            memory_accesses: Default::default(),
            ebs_storage: Default::default(),
            ebs_occupancy: Default::default(),
            ebs_accesses: Default::default(),
            user_latency: Default::default(),
            user_throughput: Default::default(),
            latency_miss_ratio: Default::default(),
            global_hash_rings: {
                let mut map = HashMap::new();
                map.insert(Tier::Memory, Default::default());
                map.insert(Tier::Disk, Default::default());
                map
            },
            local_hash_rings: {
                let mut map = HashMap::new();
                map.insert(Tier::Memory, Default::default());
                map.insert(Tier::Disk, Default::default());
                map
            },
            key_replication_map: Default::default(),
            server_monitoring_epoch: Default::default(),
            memory_node_count: Default::default(),
            ebs_node_count: Default::default(),
            summary_stats: Default::default(),
            request_id: Default::default(),
            removing_memory_node: Default::default(),
            removing_ebs_node: Default::default(),
        };

        monitor.run_inner().await?;

        Ok(())
    }
}

impl<'a> MonitoringNode<'a> {
    async fn run_inner(mut self) -> eyre::Result<()> {
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

        // responsible for both node join and departure
        let mut notify_subscriber = zenoh
            .subscribe(&MonitoringThread::notify_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare notify subscriber")?;
        let mut notify_stream = notify_subscriber.receiver().fuse();

        // responsible for receiving depart done notice
        let mut depart_done_subscriber = zenoh
            .subscribe(&self.mt.depart_done_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare depart_done subscriber")?;
        let mut depart_done_stream = depart_done_subscriber.receiver().fuse();

        // responsible for receiving feedback from users
        let mut feedback_subscriber = zenoh
            .subscribe(&MonitoringThread::feedback_report_topic(&self.zenoh_prefix))
            .await
            .map_err(|e| eyre::eyre!(e))
            .context("failed to declare notify subscriber")?;
        let mut feedback_stream = feedback_subscriber.receiver().fuse();

        loop {
            futures::select! {
                sample = notify_stream.select_next_some() => {
                    let serialized = sample.value.as_string()?;
                    self.membership_handler(&serialized).await.context("notify handler failed")?;
                }
                sample = depart_done_stream.select_next_some() => {
                    let serialized = sample.value.as_string()?;
                    self.depart_done_handler(&serialized).await.context("depart done handler failed")?;
                }
                sample = feedback_stream.select_next_some() => {
                    let serialized = sample.value.as_string()?;
                    self.feedback_handler(&serialized).await.context("feedback handler failed")?;
                }
                complete => break,
            }
        }

        log::info!("monitor task is finished");

        Ok(())
    }

    async fn collect_stats(&mut self) -> eyre::Result<()> {
        let report_end = Instant::now();

        if report_end - self.report_start >= MONITORING_PERIOD {
            self.server_monitoring_epoch += 1;

            self.memory_node_count = self.global_hash_rings[&Tier::Memory].unique_nodes().len();
            self.ebs_node_count = self.global_hash_rings[&Tier::Disk].unique_nodes().len();

            self.key_access_frequency.clear();
            self.key_access_summary.clear();

            self.memory_storage.clear();
            self.ebs_storage.clear();

            self.memory_occupancy.clear();
            self.ebs_occupancy.clear();

            self.summary_stats.clear();

            self.user_latency.clear();
            self.user_throughput.clear();
            self.latency_miss_ratio.clear();

            self.collect_internal_stats().await?;
            self.compute_summary_stats();
            self.collect_external_stats();

            // initialize replication factor for new keys
            for key in self.key_access_summary.keys().cloned().collect::<Vec<_>>() {
                if let Key::Client(key) = key {
                    if !self.key_replication_map.contains_key(&key) {
                        self.init_replication(key);
                    }
                }
            }

            self.storage_policy().await?;
            self.movement_policy().await?;
            self.slo_policy().await?;

            self.report_start = Instant::now();
        }

        Ok(())
    }

    fn prepare_metadata_get_request(
        &mut self,
        key: &MetadataKey,
        addr_request_map: &mut HashMap<String, Request>,
    ) -> Result<(), eyre::Error> {
        let threads = self.hash_ring_util.get_responsible_threads_metadata(
            key,
            &self.global_hash_rings[&Tier::Memory],
            &self.local_hash_rings[&Tier::Memory],
        )?;
        if !threads.is_empty() {
            let target_address = threads
                .choose(&mut rand::thread_rng())
                .unwrap()
                .request_topic(&self.zenoh_prefix);
            if let hash_map::Entry::Vacant(entry) = addr_request_map.entry(target_address) {
                let response_addr = self.mt.response_topic(&self.zenoh_prefix);
                entry.insert(Request {
                    request: RequestData::Get {
                        keys: vec![key.clone().into()],
                    },
                    // NB: response_address might not be necessary here
                    // (or in other places where req_id is constructed either).
                    request_id: Some(format!("{}:{}", response_addr, self.request_id)),
                    response_address: Some(response_addr.to_string()),
                    address_cache_size: Default::default(),
                });
                self.request_id += 1;
            }
        };
        Ok(())
    }

    fn prepare_metadata_put_request(
        &mut self,
        key: &MetadataKey,
        value: Vec<u8>,
        addr_request_map: &mut HashMap<String, Request>,
    ) -> Result<(), eyre::Error> {
        let threads = self.hash_ring_util.get_responsible_threads_metadata(
            key,
            &self.global_hash_rings[&Tier::Memory],
            &self.local_hash_rings[&Tier::Memory],
        )?;
        if !threads.is_empty() {
            let target_address = threads
                .choose(&mut rand::thread_rng())
                .unwrap()
                .request_topic(&self.zenoh_prefix);
            if let hash_map::Entry::Vacant(entry) = addr_request_map.entry(target_address) {
                let response_addr = self.mt.response_topic(&self.zenoh_prefix);
                entry.insert(Request {
                    request: RequestData::Put {
                        tuples: vec![PutTuple {
                            key: key.clone().into(),
                            value: LatticeValue::Lww(LastWriterWinsLattice::from_pair(
                                Timestamp::now(),
                                value,
                            )),
                        }],
                    },
                    // NB: response_address might not be necessary here
                    // (or in other places where req_id is constructed either).
                    request_id: Some(format!("{}:{}", response_addr, self.request_id)),
                    response_address: Some(response_addr.to_string()),
                    address_cache_size: Default::default(),
                });
                self.request_id += 1;
            }
        };
        Ok(())
    }

    async fn wait_for_response(&mut self) -> eyre::Result<Response> {
        let raw_response = self
            .responses
            .receiver()
            .next()
            .await
            .ok_or_else(|| anyhow!("response stream closed unexpectedly"))?;
        let response: Response = serde_json::from_str(&raw_response.value.as_string()?)?;

        response.error?;

        Ok(response)
    }

    fn init_replication(&mut self, key: ClientKey) {
        let entry = self.key_replication_map.entry(key).or_default();
        for &tier in ALL_TIERS {
            entry.global_replication.insert(
                tier,
                self.config_data.tier_metadata[&tier].default_replication,
            );
            entry
                .local_replication
                .insert(tier, self.config_data.default_local_replication);
        }
    }

    async fn add_node(&self, tier: Tier, number: usize) -> eyre::Result<()> {
        if let Some(management_node) = &self.management_node {
            log::info!("Adding {} node(s) in tier {}.", number, tier_name(tier));

            self.zenoh
                .put(
                    &management_node.add_nodes_topic(&self.zenoh_prefix),
                    serde_json::to_string(&AddNodes { tier, number })?,
                )
                .await
                .map_err(|e| eyre::eyre!(e))?;
        } else {
            log::warn!("Cannot add node since no management node is configured");
        }
        Ok(())
    }

    async fn remove_node(&mut self, node_id: String, tier: Tier) -> eyre::Result<()> {
        // notify thread 0 of departure, which will forward this message to its other threads
        let connection_addr = KvsThread {
            node_id: node_id.clone(),
            thread_id: 0,
        }
        .self_depart_topic(&self.zenoh_prefix);
        // we expect one depart done message per thread
        self.departing_node_map
            .insert(node_id, self.config_data.tier_metadata[&tier].thread_number);
        // the depart done message should be sent to our depart done topic
        let self_depart = SelfDepart {
            response_topic: self.mt.depart_done_topic(&self.zenoh_prefix).to_string(),
        };

        self.zenoh
            .put(&connection_addr, serde_json::to_string(&self_depart)?)
            .await
            .map_err(|e| eyre::eyre!(e))?;

        Ok(())
    }

    async fn change_replication_factor(
        &mut self,
        requests: HashMap<ClientKey, KeyReplication>,
    ) -> eyre::Result<()> {
        // used to keep track of the original replication factors for the requested
        // keys
        let mut orig_key_replication_map_info: HashMap<ClientKey, KeyReplication> =
            Default::default();

        // store the new replication factor synchronously in storage servers
        let mut addr_request_map: HashMap<String, Request> = Default::default();

        // form the replication factor update request map
        let mut replication_factor_map: HashMap<String, ReplicationFactorUpdate> =
            Default::default();

        for (key, new_rep) in requests.clone() {
            orig_key_replication_map_info
                .insert(key.clone(), self.key_replication_map[&key].clone());

            // don't send an update if we're not changing the metadata
            if new_rep == self.key_replication_map[&key] {
                continue;
            }

            // update the metadata map
            self.key_replication_map.insert(key.clone(), new_rep);

            // prepare data to be stored in the storage tier
            let rep_data = ReplicationFactor {
                key: key.clone(),
                global: self.key_replication_map[&key]
                    .global_replication
                    .iter()
                    .map(|(&tier, &value)| ReplicationValue { tier, value })
                    .collect(),
                local: self.key_replication_map[&key]
                    .local_replication
                    .iter()
                    .map(|(&tier, &value)| ReplicationValue { tier, value })
                    .collect(),
            };

            let rep_key = MetadataKey::Replication { key: key.clone() };

            self.prepare_metadata_put_request(
                &rep_key,
                serde_json::to_vec(&rep_data)?,
                &mut addr_request_map,
            )?;
        }

        // send updates to storage nodes
        let mut expected_response_ids = HashSet::new();
        let mut failed_keys = HashSet::new();
        for (address, request) in addr_request_map {
            let serialized_req =
                serde_json::to_string(&request).context("failed to serialize KeyRequest")?;
            self.zenoh
                .put(&address, serialized_req)
                .await
                .map_err(|e| eyre::eyre!(e))?;

            expected_response_ids.insert(request.request_id.unwrap());
        }

        // wait for replies
        while !expected_response_ids.is_empty() {
            let response = self.wait_for_response().await?; // TODO handle timeout via failed_keys?
            let response_id = match response.response_id {
                Some(id) => id,
                None => {
                    log::warn!("ignoring unexpected response without ID");
                    continue;
                }
            };

            if expected_response_ids.take(&response_id).is_none() {
                log::warn!("ignoring unexpected response with ID `{}`", response_id);
                continue;
            }

            for tuple in response.tuples {
                match tuple.error {
                    Some(AnnaError::WrongThread) => {
                        log::error!(
                            "Replication factor put for key {:?} rejected due to incorrect address.",
                            tuple.key
                        );

                        let client_key =
                            if let Key::Metadata(MetadataKey::Replication { key }) = tuple.key {
                                key
                            } else {
                                bail!("invalid key in response tuple");
                            };
                        failed_keys.insert(client_key);
                    }
                    Some(error) => return Err(error.into()),
                    None => {}
                }
            }
        }

        for key in &failed_keys {
            for &tier in ALL_TIERS {
                let rep = usize::max(
                    self.key_replication_map[key].global_replication[&tier],
                    orig_key_replication_map_info[key].global_replication[&tier],
                );
                let responsible_nodes: Vec<_> = self.global_hash_rings[&tier]
                    .responsible_nodes(&key.clone().into(), rep)
                    .into_iter()
                    .map(String::from)
                    .collect();

                for node_id in responsible_nodes {
                    self.prepare_replication_factor_update(
                        key.clone(),
                        &mut replication_factor_map,
                        KvsThread {
                            node_id: node_id.to_owned(),
                            thread_id: 0,
                        }
                        .replication_change_topic(&self.zenoh_prefix),
                    );
                }
            }

            // form replication factor update requests for routing nodes
            for routing_node in &self.routing_node_ids.clone() {
                self.prepare_replication_factor_update(
                    key.clone(),
                    &mut replication_factor_map,
                    RoutingThread {
                        node_id: routing_node.clone(),
                        thread_id: 0,
                    }
                    .replication_change_topic(&self.zenoh_prefix),
                );
            }
        }

        // send replication factor update to all relevant nodes
        for (addr, rep_factor) in replication_factor_map {
            let serialized = serde_json::to_string(&rep_factor)?;
            self.zenoh
                .put(&addr, serialized)
                .await
                .map_err(|e| eyre::eyre!(e))?;
        }

        // restore rep factor for failed keys
        for key in failed_keys {
            let orig_val = orig_key_replication_map_info[&key].clone();
            self.key_replication_map.insert(key, orig_val);
        }

        Ok(())
    }

    fn prepare_replication_factor_update(
        &mut self,
        key: ClientKey,
        replication_factor_map: &mut HashMap<String, ReplicationFactorUpdate>,
        replication_change_topic: String,
    ) {
        let global = self.key_replication_map[&key]
            .global_replication
            .iter()
            .map(|(&tier, &value)| ReplicationValue { tier, value })
            .collect();

        let local = self.key_replication_map[&key]
            .local_replication
            .iter()
            .map(|(&tier, &value)| ReplicationValue { tier, value })
            .collect();

        replication_factor_map
            .entry(replication_change_topic)
            .or_default()
            .updates
            .push(ReplicationFactor { key, global, local });
    }
}
