use crate::{
    lattice::{last_writer_wins::Timestamp, LastWriterWinsLattice},
    messages::{
        key_data::{KeyAccessData, KeyCount, KeySizeData},
        management::FuncNodesQuery,
        request::{PutTuple, RequestData},
        user_feedback::ServerThreadStatistics,
        Request, Tier,
    },
    metadata::{KvsMetadataKind, MetadataKey},
    store::LatticeValue,
    topics::{KvsThread, ManagementThread},
    Key,
};
use eyre::Context;
use std::{
    collections::{BTreeSet, HashMap},
    time::{Duration, Instant},
};

// define server's key monitoring threshold
const K_KEY_MONITORING_THRESHOLD: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct ReportData {
    /// Keeps track of the start time of the current report period.
    ///
    /// Each KVS periodically sends a report with different statistics. This field specifies the
    /// start time of the current report period. It is updated with the current time after
    /// a new report was sent out.
    report_start: Instant,
    /// Keeps track of the total time that the node was running (e.g. handling some messages).
    ///
    /// Sent as part of the periodic report and reset afterwards.
    working_time: Duration,
    /// Keeps track time that the node was running to handle specific messages.
    ///
    /// The current implementation hardcodes an index into this array for each message.
    ///
    /// Sent as part of the periodic report and reset afterwards.
    working_time_map: [Duration; 10],
    /// Increased whenever a key was accessed.
    ///
    /// Sent as part of the periodic report and reset afterwards.
    access_count: usize,
    /// Per-key access statistics, including timestamps for each access.
    key_access_tracker: HashMap<Key, BTreeSet<Instant>>,
    /// Sent as part of the periodic report and increased by one afterwards.
    epoch: usize,

    /// The ID of the management node, if any.
    management_id: Option<String>,
}

impl ReportData {
    pub fn new(management_id: Option<String>) -> ReportData {
        Self {
            report_start: Instant::now(),
            working_time: Default::default(),
            working_time_map: Default::default(),
            epoch: 0,
            access_count: 0,
            key_access_tracker: Default::default(),
            management_id,
        }
    }

    pub fn report_start(&self) -> Instant {
        self.report_start
    }

    #[cfg(test)]
    pub fn access_count(&self) -> usize {
        self.access_count
    }

    #[cfg(test)]
    pub fn key_access_count(&self, key: &Key) -> usize {
        self.key_access_tracker
            .get(key)
            .map(|s| s.len())
            .unwrap_or_default()
    }

    pub fn record_working_time(&mut self, time_elapsed: Duration, index: usize) {
        self.working_time += time_elapsed;
        self.working_time_map[index] += time_elapsed;
    }

    pub fn record_key_access(&mut self, key: &Key, access_time: Instant) {
        self.key_access_tracker
            .entry(key.clone())
            .or_default()
            .insert(access_time);

        self.access_count += 1;
    }

    /// Starts the next reporting epoch and returns the report messages that should be sent out.
    pub async fn next_epoch(
        &mut self,
        duration: Duration,
        node_tier: Tier,
        node: &KvsThread,
        primary_key_size: KeySizeData,
        zenoh: &zenoh::Session,
        zenoh_prefix: &str,
        wt: &KvsThread,
    ) -> eyre::Result<Vec<ReportMessage>> {
        self.epoch += 1;
        let ts = Timestamp::now();

        let stat = self.create_stat_report(duration, node_tier, node, ts)?;
        let access = self.create_access_report(node_tier, node, ts)?;
        let size = self.create_size_report(node_tier, node, ts, primary_key_size)?;

        if let Some(management_id) = &self.management_id {
            zenoh
                .put(
                    &ManagementThread {
                        node_id: management_id.clone(),
                    }
                    .query_func_nodes_topic(zenoh_prefix),
                    serde_json::to_string(&FuncNodesQuery {
                        response_topic: wt.management_node_response_topic(zenoh_prefix).to_string(),
                    })?,
                )
                .await
                .map_err(|e| eyre::eyre!(e))?;
        }

        self.report_start = Instant::now();
        self.access_count = 0;
        self.working_time = Default::default();
        self.working_time_map = Default::default();

        Ok(vec![stat, access, size])
    }

    fn create_stat_report(
        &mut self,
        duration: Duration,
        node_tier: Tier,
        node: &KvsThread,
        ts: Timestamp,
    ) -> eyre::Result<ReportMessage> {
        let key = MetadataKey::KvsThread {
            tier: node_tier,
            kvs_thread: node.clone(),
            kind: KvsMetadataKind::ServerStats,
        };
        let consumption: u64 = 0;
        let mut index = 0;
        for time in &self.working_time_map {
            let event_occupancy = time.as_secs_f64() / duration.as_secs_f64();

            if event_occupancy > 0.02 {
                log::info!("Event {} occupancy is {}.", index, event_occupancy);
                index += 1;
            }
        }
        let occupancy = self.working_time.as_secs_f64() / duration.as_secs_f64();
        if occupancy > 0.02 {
            log::info!("Occupancy is {}.", occupancy);
        }
        let stat = ServerThreadStatistics {
            storage_consumption: consumption / 1000, // cast to KB
            occupancy,
            epoch: self.epoch,
            access_count: self.access_count,
        };
        let serialized_stat =
            serde_json::to_vec(&stat).context("failed to serialize ServerThreadStatistics")?;
        let stat_req = Request {
            request: RequestData::Put {
                tuples: vec![PutTuple {
                    key: key.clone().into(),
                    value: LatticeValue::Lww(LastWriterWinsLattice::from_pair(ts, serialized_stat)),
                }],
            },
            response_address: Default::default(),
            request_id: Default::default(),
            address_cache_size: Default::default(),
        };
        Ok(ReportMessage {
            key,
            message: stat_req,
        })
    }

    fn create_access_report(
        &mut self,
        node_tier: Tier,
        node: &KvsThread,
        ts: Timestamp,
    ) -> eyre::Result<ReportMessage> {
        let mut access = KeyAccessData::default();
        let current_time = Instant::now();
        for (key, access_times) in &mut self.key_access_tracker {
            // garbage collect
            for &time in access_times.iter() {
                if current_time - time >= K_KEY_MONITORING_THRESHOLD {
                    access_times.remove(&time);
                    break;
                }
            }

            // update key_access_frequency
            let tp = KeyCount {
                key: key.clone(),
                access_count: access_times.len(),
            };
            access.keys.push(tp);
        }
        let key = MetadataKey::KvsThread {
            tier: node_tier,
            kvs_thread: node.clone(),
            kind: KvsMetadataKind::KeyAccess,
        };
        let serialized_access =
            serde_json::to_vec(&access).context("failed to serialize KeyAccessData")?;
        let access_req = Request {
            request: RequestData::Put {
                tuples: vec![PutTuple {
                    key: key.clone().into(),
                    value: LatticeValue::Lww(LastWriterWinsLattice::from_pair(
                        ts,
                        serialized_access,
                    )),
                }],
            },
            response_address: Default::default(),
            request_id: Default::default(),
            address_cache_size: Default::default(),
        };
        Ok(ReportMessage {
            key,
            message: access_req,
        })
    }

    fn create_size_report(
        &mut self,
        node_tier: Tier,
        node: &KvsThread,
        ts: Timestamp,
        primary_key_size: KeySizeData,
    ) -> eyre::Result<ReportMessage> {
        let key = MetadataKey::KvsThread {
            tier: node_tier,
            kvs_thread: node.clone(),
            kind: KvsMetadataKind::KeySize,
        };
        let serialized_size =
            serde_json::to_vec(&primary_key_size).context("failed to serialize KeySizeData")?;
        let size_req = Request {
            request: RequestData::Put {
                tuples: vec![PutTuple {
                    key: key.clone().into(),
                    value: LatticeValue::Lww(LastWriterWinsLattice::from_pair(ts, serialized_size)),
                }],
            },
            response_address: Default::default(),
            request_id: Default::default(),
            address_cache_size: Default::default(),
        };
        Ok(ReportMessage {
            key,
            message: size_req,
        })
    }
}

/// A report message that should be sent out.
pub struct ReportMessage {
    /// The message should be sent to a thread that is responsible for this metadata key.
    pub key: MetadataKey,
    /// The message that should be sent.
    pub message: Request,
}
