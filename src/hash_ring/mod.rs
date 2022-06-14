//! Hash ring types that define which node is responsible for a key.

use self::consistent_hash_map::{ConsistentHashMap, VirtualNode, VirtualThread};
use crate::{
    messages::{request::RequestData, Request, TcpMessage, Tier},
    metadata::MetadataKey,
    nodes::send_tcp_message,
    topics::KvsThread,
    ClientKey, Key,
};
use eyre::{anyhow, eyre, Context};
use rand::prelude::SliceRandom;
use smol::net::TcpStream;
use std::collections::{HashMap, HashSet};

mod consistent_hash_map;

/// Defines the maximum number of servers that should be returned from `responsible_servers`
/// for a key.
const METADATA_REPLICATION_FACTOR: usize = 1;

/// Sets the number of virtual entries that should be created per node/thread in hash rings.
///
/// The virtual nodes/threads are spread uniformly across the hash ring using consistent
/// hashing. This way keys are uniformly distributed across nodes/nodes even if the keys are
/// not uniformly distributed.
const VIRTUAL_ENTRY_NUM: u32 = 3000;

/// Utility type for getting the responsible nodes and threads based on the configured
/// local replication.
pub struct HashRingUtil {
    /// The local replication factor, typically set via the config file.
    default_local_replication: usize,
}

impl HashRingUtil {
    /// Creates a new instance based on the given local replication value.
    pub fn new(default_local_replication: usize) -> Self {
        Self {
            default_local_replication,
        }
    }

    /// Tries to get all threads responsible for a [`Key`].
    ///
    /// If the responsible threads cannot be determined, a replication factor request is
    /// issued and `None` is returned.
    ///
    /// ## Implementation Notes
    ///
    /// The behavior of this convenience method depends on whether the given `key` is a metadata
    /// key. If it is, the call is forwarded to
    /// [`Self::get_responsible_threads_metadata`]. Else, it is forwarded to
    /// [`Self::try_get_responsible_threads_for_client_key`].
    #[allow(clippy::too_many_arguments)]
    pub async fn try_get_responsible_threads(
        &self,
        response_address: String,
        key: Key,
        global_hash_rings: &HashMap<Tier, GlobalHashRing>,
        local_hash_rings: &HashMap<Tier, LocalHashRing>,
        key_replication_map: &HashMap<ClientKey, KeyReplication>,
        tiers: &[Tier],
        zenoh: &zenoh::Session,
        zenoh_prefix: &str,
        node_connections: &mut HashMap<KvsThread, TcpStream>,
    ) -> eyre::Result<Option<Vec<KvsThread>>> {
        let empty_local_hash_ring = LocalHashRing::default();
        match key {
            Key::Metadata(key) => self
                .get_responsible_threads_metadata(
                    &key,
                    global_hash_rings
                        .get(&Tier::Memory)
                        .ok_or_else(|| anyhow!("no global hash ring for tier"))?,
                    local_hash_rings
                        .get(&Tier::Memory)
                        .unwrap_or(&empty_local_hash_ring),
                )
                .map(Some),
            Key::Client(key) => {
                self.try_get_responsible_threads_for_client_key(
                    response_address,
                    key,
                    global_hash_rings,
                    local_hash_rings,
                    key_replication_map,
                    tiers,
                    zenoh,
                    zenoh_prefix,
                    node_connections,
                )
                .await
            }
        }
    }

    /// Tries to get all threads responsible for a [`ClientKey`].
    ///
    /// If the responsible threads cannot be determined, a replication factor request is
    /// issued and `None` is returned.
    ///
    /// ## Implementation Notes
    ///
    /// First, we look up the key replication in the given `key_replication_map` and forward
    /// the call to [`get_responsible_threads`] if the key replication is known. Else, we
    /// invoke [`Self::issue_replication_factor_request`] and return `Ok(None)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_get_responsible_threads_for_client_key(
        &self,
        response_address: String,
        key: ClientKey,
        global_hash_rings: &HashMap<Tier, GlobalHashRing>,
        local_hash_rings: &HashMap<Tier, LocalHashRing>,
        key_replication_map: &HashMap<ClientKey, KeyReplication>,
        tiers: &[Tier],
        zenoh: &zenoh::Session,
        zenoh_prefix: &str,
        node_connections: &mut HashMap<KvsThread, TcpStream>,
    ) -> eyre::Result<Option<Vec<KvsThread>>> {
        if let Some(key_replication) = key_replication_map.get(&key) {
            Ok(Some(get_responsible_threads_across_tiers(
                &key,
                key_replication,
                tiers,
                global_hash_rings,
                local_hash_rings,
            )?))
        } else {
            let empty_local_hash_ring = LocalHashRing::default();
            self.issue_replication_factor_request(
                response_address,
                key,
                global_hash_rings
                    .get(&Tier::Memory)
                    .ok_or_else(|| anyhow!("no global hash ring for tier"))?,
                local_hash_rings
                    .get(&Tier::Memory)
                    .unwrap_or(&empty_local_hash_ring),
                zenoh,
                zenoh_prefix,
                node_connections,
            )
            .await
            .context("failed to issue replication factor request")?;

            Ok(None)
        }
    }

    /// Returns the responsible threads for the given metadata key.
    pub fn get_responsible_threads_metadata(
        &self,
        key: &MetadataKey,
        global_memory_hash_ring: &GlobalHashRing,
        local_memory_hash_ring: &LocalHashRing,
    ) -> eyre::Result<Vec<KvsThread>> {
        get_responsible_threads(
            &key.clone().into(),
            METADATA_REPLICATION_FACTOR,
            self.default_local_replication,
            global_memory_hash_ring,
            local_memory_hash_ring,
        )
    }

    /// Sends a replication factor request to a random responsible thread.
    ///
    /// Sends a [`Request`] message of type `GET` for the replication metadata key
    /// that corresponds to the given `key`. The response will be sent to the
    /// specified `response_address`. The hash ring parameters are needed for finding
    /// the responsible threads.
    pub async fn issue_replication_factor_request(
        &self,
        response_address: String,
        key: ClientKey,
        global_memory_hash_ring: &GlobalHashRing,
        local_memory_hash_ring: &LocalHashRing,
        zenoh: &zenoh::Session,
        zenoh_prefix: &str,
        node_connections: &mut HashMap<KvsThread, TcpStream>,
    ) -> eyre::Result<()> {
        log::info!("issuing replication factor request for key {}", key);
        let replication_key = MetadataKey::Replication { key };
        let threads = self.get_responsible_threads_metadata(
            &replication_key,
            global_memory_hash_ring,
            local_memory_hash_ring,
        )?;

        let kvs_thread = threads
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| anyhow!("no responsible threads"))?;
        let target_address = kvs_thread.request_topic(zenoh_prefix);

        let key_request = Request {
            request: RequestData::Get {
                keys: vec![replication_key.into()],
            },
            response_address: Some(response_address.to_string()),
            request_id: None,
            address_cache_size: Default::default(),
        };

        if let Some(connection) = node_connections.get_mut(kvs_thread) {
            send_tcp_message(&TcpMessage::Request(key_request), connection)
                .await
                .context("failed to send key request via TCP")?;
        } else {
            let serialized =
                serde_json::to_string(&key_request).context("failed to serialize KeyRequest")?;

            zenoh
                .put(&target_address, serialized)
                .await
                .map_err(|e| eyre!(e))
                .context("failed to send replication factor request")?;
        }

        Ok(())
    }
}

/// Returns the [`KvsThread`]s that are responsible for the given key.
///
/// Reads the responsible node IDs from the given `global_hash_ring` using
/// the `global_replication` factor and the responsible thread IDs from the
/// given `local_hash_ring` using the `local_replication` factor. Returns
/// the [cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) of
/// these two sets, i.e. all combinations of node id and thread id.
pub fn get_responsible_threads(
    key: &Key,
    global_replication: usize,
    local_replication: usize,
    global_hash_ring: &GlobalHashRing,
    local_hash_ring: &LocalHashRing,
) -> Result<Vec<KvsThread>, eyre::Error> {
    // get the responsible nodes from the global hash ring first
    let nodes = global_hash_ring.responsible_nodes(key, global_replication);
    // next, get the IDs of the responsible threads on each node
    let thread_ids = local_hash_ring.responsible_threads(key, local_replication);

    // add the cartesian product of nodes and threads to the `result` set
    let mut result = Vec::new();
    for node_id in nodes {
        for &thread_id in &thread_ids {
            result.push(KvsThread::new(node_id.to_owned(), thread_id));
        }
    }
    Ok(result)
}

/// Returns the [`KvsThread`]s that are responsible for the given key, across the given tiers.
///
/// Reads the responsible node IDs from the given `global_hash_ring` and the responsible thread
/// IDs from the given `local_hash_ring`, using the `key_replication` factors. Returns
/// the [cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) of
/// these two sets, i.e. all combinations of node id and thread id.
pub fn get_responsible_threads_across_tiers(
    key: &ClientKey,
    key_replication: &KeyReplication,
    tiers: &[Tier],
    global_hash_rings: &HashMap<Tier, GlobalHashRing>,
    local_hash_rings: &HashMap<Tier, LocalHashRing>,
) -> eyre::Result<Vec<KvsThread>> {
    let mut result = Vec::new();
    for &tier in tiers {
        let empty_global_hash_ring = GlobalHashRing::default();
        let global_hash_ring = global_hash_rings
            .get(&tier)
            .unwrap_or(&empty_global_hash_ring);
        let empty_local_hash_ring = LocalHashRing::default();
        let local_hash_ring = local_hash_rings
            .get(&tier)
            .unwrap_or(&empty_local_hash_ring);

        let global_replication = key_replication
            .global_replication
            .get(&tier)
            .copied()
            .unwrap_or_default();
        let local_replication = key_replication
            .local_replication
            .get(&tier)
            .copied()
            .unwrap_or_default();

        result.extend_from_slice(&get_responsible_threads(
            &key.clone().into(),
            global_replication,
            local_replication,
            global_hash_ring,
            local_hash_ring,
        )?);
    }
    Ok(result)
}

/// Determines which nodes are responsible for each key.
///
/// Uses a consistent hash map and virtual nodes to spread keys uniformly across nodes.
#[derive(Default)]
pub struct GlobalHashRing {
    consistent_hash_map: ConsistentHashMap<VirtualNode>,
    unique_nodes: HashSet<String>,
    node_join_count: HashMap<String, u32>,
}

impl GlobalHashRing {
    /// Inserts the given node ID into the ring.
    ///
    /// Returns `true` if the node was not present in the ring before, or if the `join_count`
    /// of the previous entry is lower than the new `join_count`.
    pub fn insert_node(&mut self, node_id: String, join_count: u32) -> bool {
        if self.unique_nodes.contains(&node_id) {
            // if we already have the server, only return true if it's rejoining
            let entry = self.node_join_count.entry(node_id).or_default();
            if *entry < join_count {
                *entry = join_count;
                true
            } else {
                false
            }
        } else {
            // otherwise, insert it into the hash ring for the first time
            self.unique_nodes.insert(node_id.clone());
            self.node_join_count.insert(node_id.clone(), join_count);

            for virtual_num in 0..VIRTUAL_ENTRY_NUM {
                let virtual_node = VirtualNode::new(node_id.clone(), virtual_num);
                self.consistent_hash_map.insert(virtual_node);
            }

            true
        }
    }

    /// Returns the full set of node IDs that are stored in this ring.
    pub fn unique_nodes(&self) -> &HashSet<String> {
        &self.unique_nodes
    }

    /// Returns the total number of _virtual_ nodes in the ring.
    ///
    /// Each node is represented by several virtual nodes in the ring to ensure that keys
    /// are uniformly spread across nodes. For this reason, the length returned from this
    /// function will be significantly larger than the size of the set returned by
    /// [`Self::unique_nodes`].
    pub fn len(&self) -> usize {
        self.consistent_hash_map.len()
    }

    /// Returns true if the ring is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the set of node IDs responsible for the given key.
    pub fn responsible_nodes(&self, key: &Key, global_rep: usize) -> HashSet<&str> {
        let mut unique_servers = HashSet::new();

        for server in self.consistent_hash_map.entries_starting_at(key) {
            let new = unique_servers.insert(server.node_id());
            if new && unique_servers.len() >= global_rep {
                break;
            }
        }

        unique_servers
    }

    /// Finds the ID of the first node responsible for the given key.
    ///
    /// Returns `None` if the hash ring is empty.
    pub fn find(&self, key: &Key) -> Option<&str> {
        self.consistent_hash_map
            .entries_starting_at(key)
            .next()
            .map(|n| n.node_id())
    }

    /// Removes the given node from the ring (including all corresponding virtual nodes).
    pub fn remove_node(&mut self, node_id: &str) {
        for virtual_num in 0..VIRTUAL_ENTRY_NUM {
            let virtual_node = VirtualNode::new(node_id.to_owned(), virtual_num);
            self.consistent_hash_map.remove(&virtual_node);
        }

        self.unique_nodes.remove(node_id);
        self.node_join_count.remove(node_id);
    }
}

/// Determines which worker threads within a node are responsible for each key.
///
/// Uses a consistent hash map and virtual nodes to spread keys uniformly across threads.
#[derive(Default)]
pub struct LocalHashRing {
    consistent_hash_map: ConsistentHashMap<VirtualThread>,
}

impl LocalHashRing {
    /// Inserts the given thread ID to the ring.
    pub fn insert_thread(&mut self, thread_id: u32) {
        for virtual_num in 0..VIRTUAL_ENTRY_NUM {
            let virtual_thread = VirtualThread::new(thread_id, virtual_num);
            self.consistent_hash_map.insert(virtual_thread);
        }
    }

    /// Finds the ID of the first thread responsible for the given key.
    ///
    /// Returns `None` if the hash ring is empty.
    pub fn find(&self, key: &Key) -> Option<u32> {
        self.consistent_hash_map
            .entries_starting_at(key)
            .next()
            .map(|n| *n.thread_id())
    }

    /// Returns the set of thread IDs responsible for the given key.
    fn responsible_threads(&self, key: &Key, local_rep: usize) -> HashSet<u32> {
        let mut thread_ids = HashSet::new();

        for thread in self.consistent_hash_map.entries_starting_at(key) {
            thread_ids.insert(*thread.thread_id());
            if thread_ids.len() >= local_rep {
                break;
            }
        }
        thread_ids
    }
}

/// Keeps track of the global and local replication factors for each tier.
#[derive(Default, Clone, PartialEq, Eq)]
pub struct KeyReplication {
    /// Replication factor for the [`GlobalHashRing`], i.e. the number of nodes that each
    /// key should be stored on.
    pub global_replication: HashMap<Tier, usize>,
    /// Replication factor for the [`LocalHashRing`], i.e. the number of threads per node
    /// that each key should be stored on.
    pub local_replication: HashMap<Tier, usize>,
}

impl KeyReplication {
    /// Utility constructor function for creating a key replication with memory and
    /// disk replication factors.
    pub fn create_new(
        global_memory: usize,
        global_disk: usize,
        local_memory: usize,
        local_disk: usize,
    ) -> Self {
        Self {
            global_replication: [(Tier::Memory, global_memory), (Tier::Disk, global_disk)]
                .iter()
                .copied()
                .collect(),
            local_replication: [(Tier::Memory, local_memory), (Tier::Disk, local_disk)]
                .iter()
                .copied()
                .collect(),
        }
    }
}

/// Convert the given [`Tier`] to an uppercase string, e.g. `MEMORY`.
pub fn tier_name(tier: Tier) -> &'static str {
    match tier {
        Tier::Memory => "MEMORY",
        Tier::Disk => "DISK",
        Tier::Routing => "ROUTING",
    }
}

/// Parse the given tier name that was previously returned from [`tier_name`].
///
/// The given string must match the output of `tier_name` exactly.
pub fn parse_tier_name(s: &str) -> eyre::Result<Tier> {
    match s {
        "MEMORY" => Ok(Tier::Memory),
        "DISK" => Ok(Tier::Disk),
        "ROUTING" => Ok(Tier::Routing),
        other => Err(anyhow!("failed to parse tier name `{}`", other)),
    }
}
