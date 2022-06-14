use super::KvsNode;
use crate::{
    messages::{
        key_data::{KeySize, KeySizeData},
        request::{PutTuple, RequestData},
        Request, Tier,
    },
    nodes::kvs::report::ReportMessage,
    store::LatticeSizeEstimate,
    topics::CacheThread,
    ClientKey, Key, ALL_TIERS,
};
use eyre::{eyre, Context};
use rand::{prelude::SliceRandom, thread_rng};
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    time::{Duration, Instant},
};

// Define the data redistribute threshold
const DATA_REDISTRIBUTE_THRESHOLD: usize = 50;

/// The interval between two gossip messages.
pub const GOSSIP_PERIOD: Duration = Duration::from_secs(10);

/// The interval between two report messages.
pub const REPORT_PERIOD: Duration = Duration::from_secs(15);

/// Gossip and reporting functions.
impl KvsNode {
    /// Gossip updates to other KVS nodes, reports statistics, and redistributes data after
    /// node joins.
    ///
    /// Gossip updates are only sent if the time since the last sent gossip exceeds
    /// [`GOSSIP_PERIOD`]. Similarly, report messages are only sent if the time since
    /// the last report exceeds [`REPORT_PERIOD`].
    pub async fn gossip_updates(&mut self) -> eyre::Result<()> {
        // gossip updates to other threads
        let gossip_end = Instant::now();
        if gossip_end - self.gossip_start >= GOSSIP_PERIOD {
            self.send_out_gossip().await?;
        }

        // Collect and store internal statistics,
        // fetch the most recent list of cache IPs,
        // and send out GET requests for the cached keys by cache IP.
        let report_end = Instant::now();
        let duration = report_end - self.report_data.report_start();
        if duration >= REPORT_PERIOD {
            self.send_out_report(duration).await?;
        }

        // redistribute data after node joins
        if !self.join_gossip_map.is_empty() {
            let mut remove_address_set = HashSet::new();
            let mut addr_keyset_map: HashMap<String, HashSet<Key>> = HashMap::new();

            for (address, key_set) in &mut self.join_gossip_map {
                // track all sent keys because we cannot modify the key_set while
                // iterating over it
                let mut sent_keys = HashSet::new();

                let addr_keyset_map_entry = addr_keyset_map.entry(address.clone()).or_default();
                for key in key_set.iter() {
                    addr_keyset_map_entry.insert(key.clone());
                    sent_keys.insert(key.clone());
                    if sent_keys.len() >= DATA_REDISTRIBUTE_THRESHOLD {
                        break;
                    }
                }

                // remove the keys we just dealt with
                for key in sent_keys {
                    key_set.remove(&key);
                }

                if key_set.is_empty() {
                    remove_address_set.insert(address.clone());
                }
            }

            for remove_address in remove_address_set {
                self.join_gossip_map.remove(&remove_address);
            }

            self.send_gossip(&addr_keyset_map).await?;

            // remove keys
            if self.join_gossip_map.is_empty() {
                for key in &self.join_remove_set {
                    self.kvs.remove(key);
                }

                self.join_remove_set.clear();
            }
        }

        Ok(())
    }

    /// Sends out gossip messages to relevant nodes and threads.
    async fn send_out_gossip(&mut self) -> eyre::Result<()> {
        let work_start = Instant::now();

        let mut addr_keyset_map: HashMap<String, HashSet<Key>> = HashMap::new();
        for key in self.local_changeset.drain() {
            // Get the threads that we need to gossip to.
            let threads = self
                .hash_ring_util
                .try_get_responsible_threads(
                    self.wt.replication_response_topic(&self.zenoh_prefix),
                    key.clone(),
                    &self.global_hash_rings,
                    &self.local_hash_rings,
                    &self.key_replication_map,
                    ALL_TIERS,
                    &self.zenoh,
                    &self.zenoh_prefix,
                    &mut self.node_connections,
                )
                .await
                .context("failed to get responsible threads")?;

            if let Some(threads) = threads {
                for thread in &threads {
                    if thread != &self.wt {
                        addr_keyset_map
                            .entry(thread.gossip_topic(&self.zenoh_prefix))
                            .or_default()
                            .insert(key.clone());
                    }
                }
            } else {
                log::error!("Missing key replication factor in gossip routine.");
            }

            // Get the caches that we need to gossip to.
            if let Some(cache_ids) = self.key_to_cache_ids.get(&key) {
                for cache_ids in cache_ids {
                    let ct = CacheThread::new(cache_ids.clone(), 0);
                    addr_keyset_map
                        .entry(ct.cache_update_topic(&self.zenoh_prefix))
                        .or_default()
                        .insert(key.clone());
                }
            }
        }

        if !addr_keyset_map.is_empty() {
            self.send_gossip(&addr_keyset_map)
                .await
                .context("failed to send gossip")?;
        }

        self.gossip_start = Instant::now();
        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 9);

        Ok(())
    }

    /// Sends out report messagess with various statistics.
    async fn send_out_report(&mut self, duration: Duration) -> eyre::Result<()> {
        let primary_key_size = KeySizeData {
            key_sizes: self.primary_key_sizes(),
        };

        let report_messages = self
            .report_data
            .next_epoch(
                duration,
                self.config_data.self_tier,
                &self.wt,
                primary_key_size,
                &self.zenoh,
                &self.zenoh_prefix,
                &self.wt,
            )
            .await?;

        for ReportMessage { key, message } in report_messages {
            let threads = self.hash_ring_util.get_responsible_threads_metadata(
                &key,
                &self.global_hash_rings[&Tier::Memory],
                &self.local_hash_rings[&Tier::Memory],
            )?;
            if !threads.is_empty() {
                let target = threads.choose(&mut thread_rng()).unwrap();
                let target_address = target.request_topic(&self.zenoh_prefix);
                let serialized = serde_json::to_string(&message)
                    .context("failed to serialize report Request message")?;
                self.zenoh
                    .put(&target_address, serialized)
                    .await
                    .map_err(|e| eyre!(e))?;
            }
        }
        Ok(())
    }

    /// Send the gossip messages for the given address keyset map.
    pub async fn send_gossip(
        &self,
        addr_keyset_map: &HashMap<String, HashSet<Key>>,
    ) -> eyre::Result<()> {
        let mut gossip_map = HashMap::new();

        for (address, keys) in addr_keyset_map {
            let mut tuples = Vec::new();
            for key in keys {
                if let Some(value) = self.kvs.get(key) {
                    tuples.push(PutTuple {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
            }
            let request = Request {
                request: RequestData::Put { tuples },
                response_address: Default::default(),
                request_id: Default::default(),
                address_cache_size: Default::default(),
            };
            gossip_map.insert(address, request);
        }

        // send gossip
        for (addr, msg) in gossip_map {
            let serialized =
                serde_json::to_string(&msg).context("failed to serialize KeyRequest")?;
            self.zenoh
                .put(&addr.clone(), serialized)
                .await
                .map_err(|e| eyre!(e))
                .context("failed to send gossip message")?;
        }

        Ok(())
    }

    /// Returns the estimated storage consumption for all stored primary key replicas.
    pub fn primary_key_sizes(&self) -> Vec<KeySize> {
        let mut ret = Vec::new();
        for (key, val) in self.kvs.iter() {
            if let Ok(key) = ClientKey::try_from(key.clone()) {
                if self.is_primary_replica(&key) {
                    ret.push(KeySize {
                        key,
                        size: val.size_estimate(),
                    });
                }
            }
        }
        ret
    }

    /// Checks if the node should be considered the main replica of the given key.
    ///
    /// Used for estimating storage consumption (non-primary replicas are ignored).
    fn is_primary_replica(&self, key: &ClientKey) -> bool {
        let self_tier = self.config_data.self_tier;
        if self
            .key_replication_map
            .get(key)
            .and_then(|replication| replication.global_replication.get(&self_tier))
            .copied()
            .unwrap_or_default()
            == 0
        {
            false
        } else {
            match self_tier {
                Tier::Memory => {}
                Tier::Routing => panic!("routing tier is invalid here"),
                Tier::Disk => {
                    // check if there is another replica in the Memory tier
                    if self
                        .key_replication_map
                        .get(key)
                        .and_then(|r| r.global_replication.get(&Tier::Memory))
                        .map(|&rep_factor| rep_factor > 0)
                        .unwrap_or(false)
                    {
                        return false;
                    }
                }
            }

            let responsible_node_id = self.global_hash_rings[&self_tier].find(&key.clone().into());
            if let Some(responsible_node_id) = responsible_node_id {
                if self.node_id.as_str() == responsible_node_id {
                    let responsible_thread_id =
                        self.local_hash_rings[&self_tier].find(&key.clone().into());
                    if let Some(responsible_thread_id) = responsible_thread_id {
                        if self.wt.thread_id == responsible_thread_id {
                            return true;
                        }
                    }
                }
            }

            false
        }
    }
}
