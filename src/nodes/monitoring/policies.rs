use rand::{prelude::IteratorRandom, thread_rng};

use super::{
    MonitoringNode, GRACE_PERIOD, KEY_PROMOTION_THRESHOLD, MIN_EBS_NODE_CONSUMPTION,
    MIN_EBS_TIER_SIZE, NODE_ADDITION_BATCH_SIZE, *,
};
use crate::{hash_ring::KeyReplication, messages::Tier, Key};
use std::{collections::HashMap, time::Instant};

impl<'a> MonitoringNode<'a> {
    /// Adds and removes nodes if necessary, e.g. because of storage consumption.
    ///
    /// Has no effect if elasticity is disabled in the config.
    pub async fn storage_policy(&mut self) -> eyre::Result<()> {
        if !self.config_data.enable_elasticity {
            return Ok(());
        }

        // add new memory nodes to increase storage capacity or compute capacity if necessary
        if self.new_memory_count == 0
            && self.summary_stats.required_memory_nodes > self.memory_node_count
        {
            let time_elapsed = Instant::now() - self.grace_start;
            if time_elapsed > GRACE_PERIOD {
                self.add_node(Tier::Memory, NODE_ADDITION_BATCH_SIZE)
                    .await?;
                self.new_memory_count = NODE_ADDITION_BATCH_SIZE
            }
        }

        if self.config_data.enable_tiering {
            // add new disk nodes to increase storage capacity if necessary
            if self.new_ebs_count == 0
                && self.summary_stats.required_ebs_nodes > self.ebs_node_count
            {
                let time_elapsed = Instant::now() - self.grace_start;
                if time_elapsed > GRACE_PERIOD {
                    self.add_node(Tier::Disk, NODE_ADDITION_BATCH_SIZE).await?;
                    self.new_ebs_count = NODE_ADDITION_BATCH_SIZE;
                }
            }

            // remove underutilized disk nodes
            if self.summary_stats.avg_ebs_consumption_percentage < MIN_EBS_NODE_CONSUMPTION
                && !self.removing_ebs_node
                && self.ebs_node_count
                    > usize::max(self.summary_stats.required_ebs_nodes, MIN_EBS_TIER_SIZE)
            {
                let time_elapsed = Instant::now() - self.grace_start;

                if time_elapsed > GRACE_PERIOD {
                    // pick a random ebs node and send remove node command
                    let node_id = self.global_hash_rings[&Tier::Disk]
                        .unique_nodes()
                        .iter()
                        .choose(&mut thread_rng())
                        .unwrap()
                        .clone();

                    self.remove_node(node_id, Tier::Disk).await?;
                    self.removing_ebs_node = true;
                }
            }
        }

        Ok(())
    }

    /// Promotes hot keys to memory tier.
    pub async fn movement_policy(&mut self) -> eyre::Result<()> {
        // replicate hot keys from disk to memory nodes if tiering is enabled
        if self.config_data.enable_tiering {
            let mut requests: HashMap<ClientKey, KeyReplication> = Default::default();
            let mut required_storage = 0;
            let mut overflow = false;

            let free_storage = (MAX_MEMORY_NODE_CONSUMPTION
                * self.config_data.tier_metadata[&Tier::Memory].node_capacity as f64
                * self.memory_node_count as f64) as u64
                - self.summary_stats.total_memory_consumption;
            for (key, &access_count) in &self.key_access_summary {
                if let Key::Client(key) = key {
                    if access_count > KEY_PROMOTION_THRESHOLD
                        && self.key_replication_map[key].global_replication[&Tier::Memory] == 0
                        && self.key_size.contains_key(key)
                    {
                        required_storage += u64::try_from(self.key_size[key]).unwrap();
                        if required_storage > free_storage {
                            overflow = true;
                        } else {
                            requests.insert(
                                key.clone(),
                                KeyReplication::create_new(
                                    self.key_replication_map[key].global_replication[&Tier::Memory]
                                        + 1,
                                    self.key_replication_map[key].global_replication[&Tier::Disk]
                                        - 1,
                                    self.key_replication_map[key].local_replication[&Tier::Memory],
                                    self.key_replication_map[key].local_replication[&Tier::Disk],
                                ),
                            );
                        }
                    }
                }
            }

            log::info!("Promoting {} keys into memory tier.", requests.len());
            self.change_replication_factor(requests).await?;

            let time_elapsed = Instant::now() - self.grace_start;

            // add new memory nodes if their capacity wasn't large enough to do all key promotions
            if self.config_data.enable_elasticity
                && overflow
                && self.new_memory_count == 0
                && time_elapsed > GRACE_PERIOD
            {
                let total_memory_node_needed = ((self.summary_stats.total_memory_consumption
                    + required_storage) as f64
                    / (MAX_MEMORY_NODE_CONSUMPTION
                        * self.config_data.tier_metadata[&Tier::Memory].node_capacity as f64))
                    .ceil() as usize;

                if total_memory_node_needed > self.memory_node_count {
                    let node_to_add = total_memory_node_needed - self.memory_node_count;
                    self.add_node(Tier::Memory, node_to_add).await?;
                    self.new_memory_count = node_to_add;
                }
            }
        }

        // demote cold keys to ebs tier
        if self.config_data.enable_tiering {
            let mut requests = HashMap::new();
            let mut required_storage = 0;

            let free_storage = (MAX_EBS_NODE_CONSUMPTION
                * self.config_data.tier_metadata[&Tier::Disk].node_capacity as f64
                * self.ebs_node_count as f64) as u64
                - self.summary_stats.total_ebs_consumption;
            let mut overflow = false;

            for (key, &access_count) in &self.key_access_summary {
                if let Key::Client(key) = key {
                    if access_count < KEY_DEMOTION_THRESHOLD
                        && self.key_replication_map[key].global_replication[&Tier::Memory] > 0
                        && self.key_size.contains_key(key)
                    {
                        required_storage += u64::try_from(self.key_size[key]).unwrap();
                        if required_storage > free_storage {
                            overflow = true;
                        } else {
                            requests.insert(
                                key.clone(),
                                KeyReplication::create_new(
                                    0,
                                    self.config_data.minimum_replica_number,
                                    1,
                                    1,
                                ),
                            );
                        }
                    }
                }
            }

            log::info!("Demoting {} keys into EBS tier.", requests.len());
            self.change_replication_factor(requests).await?;

            let time_elapsed = Instant::now() - self.grace_start;

            // add new disk nodes if capacity wasn't large enough to demote all desired keys
            if self.config_data.enable_elasticity
                && overflow
                && self.new_ebs_count == 0
                && time_elapsed > GRACE_PERIOD
            {
                let total_ebs_node_needed = ((self.summary_stats.total_ebs_consumption
                    + required_storage) as f64
                    / (MAX_EBS_NODE_CONSUMPTION
                        * self.config_data.tier_metadata[&Tier::Disk].node_capacity as f64))
                    .ceil() as usize;

                if total_ebs_node_needed > self.ebs_node_count {
                    let node_to_add = total_ebs_node_needed - self.ebs_node_count;
                    self.add_node(Tier::Disk, node_to_add).await?;
                    self.new_ebs_count = node_to_add;
                }
            }
        }

        // reduce the replication factor of some keys that are not so hot anymore
        if self.config_data.enable_selective_rep {
            let mut requests = HashMap::new();
            let minimum_rep =
                KeyReplication::create_new(1, self.config_data.minimum_replica_number - 1, 1, 1);
            for (key, &access_count) in &self.key_access_summary {
                if let Key::Client(key) = key {
                    if access_count as f64 <= self.summary_stats.key_access_mean
                        && !(self.key_replication_map[key] == minimum_rep)
                    {
                        log::info!(
                            "Key {} accessed {} times (threshold is {}).",
                            key,
                            access_count,
                            self.summary_stats.key_access_mean
                        );
                        requests.insert(
                            key.clone(),
                            KeyReplication::create_new(
                                1,
                                self.config_data.minimum_replica_number - 1,
                                1,
                                1,
                            ),
                        );
                        log::info!(
                            "Dereplication for key {}. M: {}->{}. E: {}->{}",
                            key,
                            self.key_replication_map[key].global_replication[&Tier::Memory],
                            requests[key].global_replication[&Tier::Memory],
                            self.key_replication_map[key].global_replication[&Tier::Disk],
                            requests[key].global_replication[&Tier::Disk]
                        );
                    }
                }
            }

            self.change_replication_factor(requests).await?;
        }

        Ok(())
    }

    /// Policy for meeting service level objectives.
    pub async fn slo_policy(&mut self) -> eyre::Result<()> {
        let mut requests: HashMap<ClientKey, KeyReplication> = HashMap::new();

        // check latency to trigger elasticity or selective replication
        if self.summary_stats.avg_latency > SLO_WORST && self.new_memory_count == 0 {
            log::info!(
                "Observed latency ({}) violates SLO({}).",
                self.summary_stats.avg_latency,
                SLO_WORST
            );

            // figure out if we should do hot key replication or add nodes
            if self.config_data.enable_elasticity && self.summary_stats.min_memory_occupancy > 0.15
            {
                let node_to_add = ((self.summary_stats.avg_latency / SLO_WORST - 1.)
                    * self.memory_node_count as f64)
                    .ceil() as usize;

                // trigger elasticity
                let time_elapsed = Instant::now() - self.grace_start;
                if time_elapsed > GRACE_PERIOD {
                    self.add_node(Tier::Memory, node_to_add).await?;
                    self.new_memory_count = node_to_add;
                }
            } else if self.config_data.enable_selective_rep {
                for (key, &access_count) in &self.key_access_summary {
                    if let Key::Client(key) = key {
                        if access_count as f64
                            > self.summary_stats.key_access_mean + self.summary_stats.key_access_std
                            && self.latency_miss_ratio.contains_key(key)
                        {
                            log::info!(
                                "Key {} accessed {} times (threshold is {}).",
                                key,
                                access_count,
                                self.summary_stats.key_access_mean
                                    + self.summary_stats.key_access_std
                            );
                            let mut target_rep_factor = (self.key_replication_map[key]
                                .global_replication[&Tier::Memory]
                                as f64
                                * self.latency_miss_ratio[key].0)
                                as usize;

                            if target_rep_factor
                                == self.key_replication_map[key].global_replication[&Tier::Memory]
                            {
                                target_rep_factor += 1;
                            }

                            let current_mem_rep =
                                self.key_replication_map[key].global_replication[&Tier::Memory];
                            if target_rep_factor > current_mem_rep
                                && current_mem_rep < self.memory_node_count
                            {
                                let new_mem_rep =
                                    usize::min(self.memory_node_count, target_rep_factor);
                                let new_ebs_rep = usize::max(
                                    self.config_data.minimum_replica_number - new_mem_rep,
                                    0,
                                );
                                requests.insert(
                                    key.clone(),
                                    KeyReplication::create_new(
                                        new_mem_rep,
                                        new_ebs_rep,
                                        self.key_replication_map[key].local_replication
                                            [&Tier::Memory],
                                        self.key_replication_map[key].local_replication
                                            [&Tier::Disk],
                                    ),
                                );
                                log::info!(
                                    "Global hot key replication for key {}. M: {}->{}.",
                                    key,
                                    self.key_replication_map[key].global_replication[&Tier::Memory],
                                    requests[key].global_replication[&Tier::Memory]
                                );
                            } else if usize::try_from(self.config_data.memory_thread_count).unwrap()
                                > self.key_replication_map[key].local_replication[&Tier::Memory]
                            {
                                requests.insert(
                                    key.clone(),
                                    KeyReplication::create_new(
                                        self.key_replication_map[key].global_replication
                                            [&Tier::Memory],
                                        self.key_replication_map[key].global_replication
                                            [&Tier::Disk],
                                        usize::try_from(self.config_data.memory_thread_count)
                                            .unwrap(),
                                        self.key_replication_map[key].local_replication
                                            [&Tier::Disk],
                                    ),
                                );
                                log::info!(
                                    "Local hot key replication for key {}. T: {}->{}.",
                                    key,
                                    self.key_replication_map[key].local_replication[&Tier::Memory],
                                    requests[key].local_replication[&Tier::Memory]
                                );
                            }
                        }
                    }
                }

                self.change_replication_factor(requests).await?;
            }
        } else if self.config_data.enable_elasticity
            && !self.removing_memory_node
            && self.summary_stats.min_memory_occupancy < 0.05
            && self.memory_node_count
                > usize::max(
                    self.summary_stats.required_memory_nodes,
                    K_MIN_MEMORY_TIER_SIZE,
                )
        {
            log::info!(
                "Node {} is severely underutilized.",
                self.summary_stats.min_memory_occupancy_node_id,
            );
            let time_elapsed = Instant::now() - self.grace_start;

            if time_elapsed > GRACE_PERIOD {
                // before sending remove command, first adjust relevant key's replication
                // factor
                for key in self.key_access_summary.keys() {
                    if let Key::Client(key) = key {
                        if self.key_replication_map[key].global_replication[&Tier::Memory]
                            == (self.global_hash_rings[&Tier::Memory].unique_nodes().len())
                        {
                            let new_mem_rep =
                                self.key_replication_map[key].global_replication[&Tier::Memory] - 1;
                            let new_ebs_rep = usize::max(
                                self.config_data.minimum_replica_number - new_mem_rep,
                                0,
                            );
                            requests.insert(
                                key.clone(),
                                KeyReplication::create_new(
                                    new_mem_rep,
                                    new_ebs_rep,
                                    self.key_replication_map[key].local_replication[&Tier::Memory],
                                    self.key_replication_map[key].local_replication[&Tier::Disk],
                                ),
                            );
                            log::info!(
                                "Dereplication for key {}. M: {}->{}. E: {}->{}",
                                key,
                                self.key_replication_map[key].global_replication[&Tier::Memory],
                                requests[key].global_replication[&Tier::Memory],
                                self.key_replication_map[key].global_replication[&Tier::Disk],
                                requests[key].global_replication[&Tier::Disk]
                            );
                        }
                    }
                }

                self.change_replication_factor(requests).await?;

                self.remove_node(
                    self.summary_stats.min_memory_occupancy_node_id.clone(),
                    Tier::Memory,
                )
                .await?;
                self.removing_memory_node = true;
            }
        }

        Ok(())
    }
}
