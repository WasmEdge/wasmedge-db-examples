use crate::{
    messages::replication_factor::ReplicationFactorUpdate, nodes::kvs::KvsNode, topics::KvsThread,
    Key, ALL_TIERS,
};
use eyre::Context;

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

impl KvsNode {
    /// Handles incoming replication change messages.
    pub async fn replication_change_handler(&mut self, serialized: &str) -> eyre::Result<()> {
        let work_start = Instant::now();

        log::info!("Received a replication factor change.");
        if self.thread_id == 0 {
            // tell all worker threads about the replication factor change
            for tid in 1..self.config_data.thread_num {
                self.zenoh
                    .put(
                        &KvsThread::new(self.node_id.clone(), tid)
                            .replication_change_topic(&self.zenoh_prefix),
                        serialized,
                    )
                    .await
                    .map_err(|e| eyre::eyre!(e))?;
            }
        }

        let rep_change: ReplicationFactorUpdate = serde_json::from_str(serialized)
            .context("failed to deserialize ReplicationFactorUpdate")?;

        let mut addr_keyset_map: HashMap<String, HashSet<Key>> = HashMap::new();
        let mut remove_set = HashSet::new();

        for key_rep in rep_change.updates {
            let key = key_rep.key.clone();

            // if this thread has the key stored before the change
            if self.kvs.contains_key(&key.clone().into()) {
                // for every key, update the replication factor and check if the node is still
                // responsible for the key
                let orig_threads = self
                    .hash_ring_util
                    .try_get_responsible_threads_for_client_key(
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
                    .await?;

                let key_replication = self.key_replication_map.entry(key.clone()).or_default();

                if let Some(orig_threads) = orig_threads {
                    // update the replication factor
                    let mut decrement = false;

                    for global in key_rep.global {
                        if global.value < key_replication.global_replication[&global.tier] {
                            decrement = true;
                        }

                        key_replication
                            .global_replication
                            .insert(global.tier, global.value);
                    }

                    for local in key_rep.local {
                        if local.value < key_replication.local_replication[&local.tier] {
                            decrement = true;
                        }

                        key_replication
                            .local_replication
                            .insert(local.tier, local.value);
                    }

                    let threads = self
                        .hash_ring_util
                        .try_get_responsible_threads_for_client_key(
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
                        .await?;

                    if let Some(threads) = threads {
                        if !threads.contains(&self.wt) {
                            // this thread is no longer
                            // responsible for this key
                            remove_set.insert(key.clone().into());

                            // add all the new threads that this key should be sent to
                            for thread in &threads {
                                addr_keyset_map
                                    .entry(thread.gossip_topic(&self.zenoh_prefix))
                                    .or_default()
                                    .insert(key.clone().into());
                            }
                        }

                        // decrement represents whether the total global or local rep factor
                        // has been reduced; if that's not the case, and I am the "first"
                        // thread responsible for this key, then I gossip it to the new
                        // threads that are responsible for it
                        if !decrement && orig_threads.first().unwrap().node_id == self.wt.node_id {
                            let mut new_threads = HashSet::new();

                            for thread in threads {
                                if !orig_threads.contains(&thread) {
                                    new_threads.insert(thread);
                                }
                            }

                            for thread in new_threads {
                                addr_keyset_map
                                    .entry(thread.gossip_topic(&self.zenoh_prefix))
                                    .or_default()
                                    .insert(key.clone().into());
                            }
                        }
                    } else {
                        log::error!("Missing key replication factor in rep factor change routine.");
                    }
                } else {
                    log::error!("Missing key replication factor in rep factor change routine.");

                    // just update the replication factor
                    for global in key_rep.global {
                        key_replication
                            .global_replication
                            .insert(global.tier, global.value);
                    }

                    for local in key_rep.local {
                        key_replication
                            .local_replication
                            .insert(local.tier, local.value);
                    }
                }
            } else {
                let key_replication = self.key_replication_map.entry(key.clone()).or_default();

                // just update the replication factor
                for global in key_rep.global {
                    key_replication
                        .global_replication
                        .insert(global.tier, global.value);
                }

                for local in key_rep.local {
                    key_replication
                        .local_replication
                        .insert(local.tier, local.value);
                }
            }
        }

        self.send_gossip(&addr_keyset_map).await?;

        // remove keys
        for key in remove_set {
            self.kvs.remove(&key);
            self.local_changeset.remove(&key);
        }

        let time_elapsed = Instant::now() - work_start;
        self.report_data.record_working_time(time_elapsed, 6);

        Ok(())
    }
}
