use super::MonitoringNode;
use crate::{
    lattice::Lattice,
    messages::{
        key_data::{KeyAccessData, KeySizeData},
        user_feedback::ServerThreadStatistics,
        Tier,
    },
    metadata::{KvsMetadataKind, MetadataKey},
    nodes::monitoring::{MAX_EBS_NODE_CONSUMPTION, MAX_MEMORY_NODE_CONSUMPTION},
    store::LatticeValue,
    topics::KvsThread,
    ALL_TIERS,
};
use eyre::{anyhow, bail, Context};
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
};

impl<'a> MonitoringNode<'a> {
    /// Queries statistics from all KVS threads.
    pub async fn collect_internal_stats(&mut self) -> eyre::Result<()> {
        let mut addr_request_map = HashMap::new();

        // create metadata requests for all KVS nodes and threads.
        for &tier in ALL_TIERS {
            for st in self
                .global_hash_rings
                .get(&tier)
                .map(|r| r.unique_nodes())
                .cloned()
                .unwrap_or_default()
            {
                for i in 0..self.config_data.tier_metadata[&tier].thread_number {
                    self.prepare_metadata_get_request(
                        &(MetadataKey::KvsThread {
                            tier,
                            kvs_thread: KvsThread {
                                node_id: st.clone(),
                                thread_id: i,
                            },
                            kind: KvsMetadataKind::ServerStats,
                        }),
                        &mut addr_request_map,
                    )?;

                    self.prepare_metadata_get_request(
                        &(MetadataKey::KvsThread {
                            tier,
                            kvs_thread: KvsThread {
                                node_id: st.clone(),
                                thread_id: i,
                            },
                            kind: KvsMetadataKind::KeyAccess,
                        }),
                        &mut addr_request_map,
                    )?;

                    self.prepare_metadata_get_request(
                        &(MetadataKey::KvsThread {
                            tier,
                            kvs_thread: KvsThread {
                                node_id: st.clone(),
                                thread_id: i,
                            },
                            kind: KvsMetadataKind::KeySize,
                        }),
                        &mut addr_request_map,
                    )?;
                }
            }
        }

        // loop over the address request map and execute all the requests.
        let mut expected_response_ids = HashSet::new();
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
            let response = self.wait_for_response().await?;
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

            let tuple = response
                .tuples
                .get(0)
                .ok_or_else(|| anyhow!("response has no tuples"))?
                .clone();
            tuple.error.map(Err).unwrap_or(Ok(()))?;

            let key = MetadataKey::try_from(tuple.key).context("expected MetadataKey")?;
            let value = match &tuple.lattice {
                Some(LatticeValue::Lww(value)) => value.reveal(),
                None => bail!("no value in response"),
                Some(other) => bail!("unexpected response value `{:?}`", other),
            };

            match key {
                MetadataKey::KvsThread {
                    tier,
                    kvs_thread,
                    kind,
                } => match kind {
                    KvsMetadataKind::ServerStats => {
                        let stat: ServerThreadStatistics = serde_json::from_slice(value.value())
                            .context("failed to deserialize ServerThreadStatistics")?;

                        match tier {
                            Tier::Memory => {
                                self.memory_storage
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, stat.storage_consumption);
                                self.memory_occupancy
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, (stat.occupancy, stat.epoch));
                                self.memory_accesses
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, stat.access_count);
                            }
                            Tier::Disk => {
                                self.ebs_storage
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, stat.storage_consumption);
                                self.ebs_occupancy
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, (stat.occupancy, stat.epoch));
                                self.ebs_accesses
                                    .entry(kvs_thread.node_id.clone())
                                    .or_default()
                                    .insert(kvs_thread.thread_id, stat.access_count);
                            }
                            Tier::Routing => todo!(),
                        }
                    }
                    KvsMetadataKind::KeyAccess => {
                        let access: KeyAccessData = serde_json::from_slice(value.value())
                            .context("failed to deserialize KeyAccessData")?;

                        for key_count in access.keys {
                            let key = key_count.key;
                            self.key_access_frequency
                                .entry(key)
                                .or_default()
                                .insert(kvs_thread.clone(), key_count.access_count);
                        }
                    }
                    KvsMetadataKind::KeySize => {
                        let key_size_msg: KeySizeData = serde_json::from_slice(value.value())
                            .context("failed to deserialize KeySizeData")?;

                        for key_size_tuple in key_size_msg.key_sizes {
                            self.key_size
                                .insert(key_size_tuple.key, key_size_tuple.size);
                        }
                    }
                },
                other => bail!("unexpected metadata key in response: `{:?}`", other),
            }
        }

        Ok(())
    }

    pub(super) fn compute_summary_stats(&mut self) {
        // compute key access summary
        let mut cnt = 0;
        let mut mean = 0.0;
        let mut ms = 0.0;

        for (key, access) in &self.key_access_frequency {
            let mut access_count = 0;

            for &count in access.values() {
                access_count += count;
            }
            self.key_access_summary.insert(key.clone(), access_count);

            if access_count > 0 {
                cnt += 1;

                let delta = access_count as f64 - mean;
                mean += delta / cnt as f64;

                let delta2 = access_count as f64 - mean;
                ms += delta * delta2;
            }
        }

        self.summary_stats.key_access_mean = mean;
        self.summary_stats.key_access_std = (ms / cnt as f64).sqrt();

        log::info!(
            "Access: mean={}, std={}",
            self.summary_stats.key_access_mean,
            self.summary_stats.key_access_std
        );

        // compute tier access summary
        for accesses in self.memory_accesses.values() {
            for thread_access in accesses.values() {
                self.summary_stats.total_memory_access += thread_access;
            }
        }

        for access in self.ebs_accesses.values() {
            for thread_access in access.values() {
                self.summary_stats.total_ebs_access += thread_access;
            }
        }

        log::info!(
            "Total accesses: memory={}, ebs={}",
            self.summary_stats.total_memory_access,
            self.summary_stats.total_ebs_access
        );

        // compute storage consumption related statistics
        let mut m_count = 0;
        let mut e_count = 0;

        for memory_storage in &self.memory_storage {
            let mut total_thread_consumption = 0;

            for &thread_storage in memory_storage.1.values() {
                self.summary_stats.total_memory_consumption += thread_storage;
                total_thread_consumption += thread_storage;
            }

            let percentage = total_thread_consumption as f64
                / self.config_data.tier_metadata[&Tier::Memory].node_capacity as f64;
            log::info!(
                "Memory node {} storage consumption is {}.",
                memory_storage.0,
                percentage
            );

            if percentage > self.summary_stats.max_memory_consumption_percentage {
                self.summary_stats.max_memory_consumption_percentage = percentage;
            }

            m_count += 1;
        }

        for ebs_storage in &self.ebs_storage {
            let mut total_thread_consumption = 0;

            for &thread_storage in ebs_storage.1.values() {
                self.summary_stats.total_ebs_consumption += thread_storage;
                total_thread_consumption += thread_storage;
            }

            let percentage = total_thread_consumption as f64
                / self.config_data.tier_metadata[&Tier::Disk].node_capacity as f64;
            log::info!(
                "EBS node {} storage consumption is {}.",
                ebs_storage.0,
                percentage
            );

            if percentage > self.summary_stats.max_ebs_consumption_percentage {
                self.summary_stats.max_ebs_consumption_percentage = percentage;
            }
            e_count += 1;
        }

        if m_count != 0 {
            self.summary_stats.avg_memory_consumption_percentage =
                self.summary_stats.total_memory_consumption as f64
                    / (m_count as f64
                        * self.config_data.tier_metadata[&Tier::Memory].node_capacity as f64);
            log::info!(
                "Average memory node consumption is {}.",
                self.summary_stats.avg_memory_consumption_percentage
            );
            log::info!(
                "Max memory node consumption is {}.",
                self.summary_stats.max_memory_consumption_percentage
            );
        }

        if e_count != 0 {
            self.summary_stats.avg_ebs_consumption_percentage =
                self.summary_stats.total_ebs_consumption as f64
                    / (e_count as f64
                        * self.config_data.tier_metadata[&Tier::Disk].node_capacity as f64);
            log::info!(
                "Average EBS node consumption is {}.",
                self.summary_stats.avg_ebs_consumption_percentage
            );
            log::info!(
                "Max EBS node consumption is {}.",
                self.summary_stats.max_ebs_consumption_percentage
            );
        }

        self.summary_stats.required_memory_nodes = (self.summary_stats.total_memory_consumption
            as f64
            / (MAX_MEMORY_NODE_CONSUMPTION
                * self.config_data.tier_metadata[&Tier::Memory].node_capacity as f64))
            .ceil() as usize;
        self.summary_stats.required_ebs_nodes = (self.summary_stats.total_ebs_consumption as f64
            / (MAX_EBS_NODE_CONSUMPTION
                * self.config_data.tier_metadata[&Tier::Disk].node_capacity as f64))
            .ceil() as usize;

        log::info!(
            "The system requires {} new memory nodes.",
            self.summary_stats.required_memory_nodes
        );
        log::info!(
            "The system requires {} new EBS nodes.",
            self.summary_stats.required_ebs_nodes
        );

        // compute occupancy related statistics
        let mut sum_memory_occupancy = 0.0;

        let mut count = 0;

        for memory_occ in &self.memory_occupancy {
            let mut sum_thread_occupancy = 0.0;
            let mut thread_count = 0;

            for thread_occ in memory_occ.1 {
                log::info!(
                    "Memory node {} thread {} occupancy is {} at epoch {} (monitoring  \
          epoch {}).",
                    memory_occ.0,
                    thread_occ.0,
                    thread_occ.1 .0,
                    thread_occ.1 .1,
                    self.server_monitoring_epoch
                );

                sum_thread_occupancy += thread_occ.1 .0;
                thread_count += 1;
            }

            let node_occupancy = sum_thread_occupancy / thread_count as f64;
            sum_memory_occupancy += node_occupancy;

            if node_occupancy > self.summary_stats.max_memory_occupancy {
                self.summary_stats.max_memory_occupancy = node_occupancy;
            }

            if node_occupancy < self.summary_stats.min_memory_occupancy {
                self.summary_stats.min_memory_occupancy = node_occupancy;
                self.summary_stats.min_memory_occupancy_node_id = memory_occ.0.clone();
            }

            count += 1;
        }

        self.summary_stats.avg_memory_occupancy = sum_memory_occupancy / count as f64;
        log::info!(
            "Max memory node occupancy is {}.",
            self.summary_stats.max_memory_occupancy
        );
        log::info!(
            "Min memory node occupancy is {}.",
            self.summary_stats.min_memory_occupancy
        );
        log::info!(
            "Average memory node occupancy is {}.",
            self.summary_stats.avg_memory_occupancy
        );

        let mut sum_ebs_occupancy = 0.0;

        count = 0;

        for ebs_occ in &self.ebs_occupancy {
            let mut sum_thread_occupancy = 0.0;
            let mut thread_count = 0;

            for thread_occ in ebs_occ.1 {
                log::info!(
                    "EBS node {} thread {} occupancy is {} at epoch {} (monitoring epoch \
          {}).",
                    ebs_occ.0,
                    thread_occ.0,
                    thread_occ.1 .0,
                    thread_occ.1 .1,
                    self.server_monitoring_epoch
                );

                sum_thread_occupancy += thread_occ.1 .0;
                thread_count += 1;
            }

            let node_occupancy = sum_thread_occupancy / thread_count as f64;
            sum_ebs_occupancy += node_occupancy;

            if node_occupancy < self.summary_stats.min_ebs_occupancy {
                self.summary_stats.max_ebs_occupancy = node_occupancy;
            }

            if node_occupancy < self.summary_stats.min_ebs_occupancy {
                self.summary_stats.min_ebs_occupancy = node_occupancy;
            }

            count += 1;
        }

        self.summary_stats.avg_ebs_occupancy = sum_ebs_occupancy / count as f64;
        log::info!(
            "Max EBS node occupancy is {}.",
            self.summary_stats.max_ebs_occupancy
        );
        log::info!(
            "Min EBS node occupancy is {}.",
            self.summary_stats.min_ebs_occupancy
        );
        log::info!(
            "Average EBS node occupancy is {}.",
            self.summary_stats.avg_ebs_occupancy
        );
    }

    pub(super) fn collect_external_stats(&mut self) {
        // gather latency info
        if !self.user_latency.is_empty() {
            // compute latency from users
            let mut sum_latency = 0.0;
            let mut count = 0;

            for latency in self.user_latency.values() {
                sum_latency += latency;
                count += 1;
            }

            self.summary_stats.avg_latency = sum_latency / count as f64;
        }

        log::info!("Average latency is {}.", self.summary_stats.avg_latency);

        // gather throughput info
        if !self.user_throughput.is_empty() {
            // compute latency from users
            for &throughput in self.user_throughput.values() {
                self.summary_stats.total_throughput += throughput;
            }
        }

        log::info!(
            "Total throughput is {}.",
            self.summary_stats.total_throughput
        );
    }
}

#[derive(Default)]
pub struct SummaryStats {
    pub avg_latency: f64,
    pub total_throughput: f64,
    pub key_access_mean: f64,
    pub key_access_std: f64,
    pub total_memory_access: usize,
    pub total_ebs_access: usize,
    pub total_memory_consumption: u64,
    pub total_ebs_consumption: u64,
    pub required_memory_nodes: usize,
    pub required_ebs_nodes: usize,
    pub max_memory_occupancy: f64,
    pub max_ebs_occupancy: f64,
    pub min_memory_occupancy: f64,
    pub min_memory_occupancy_node_id: String,
    pub min_ebs_occupancy: f64,
    pub avg_memory_occupancy: f64,
    pub avg_ebs_occupancy: f64,
    pub max_memory_consumption_percentage: f64,
    pub max_ebs_consumption_percentage: f64,
    pub avg_memory_consumption_percentage: f64,
    pub avg_ebs_consumption_percentage: f64,
}

impl SummaryStats {
    pub fn clear(&mut self) {
        *self = Default::default();
    }
}
