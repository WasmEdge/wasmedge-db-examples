//! Feedback messages used by the benchmark executable.

use crate::ClientKey;

/// Client-generated feedback used for system monitoring and planning.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UserFeedback {
    /// A unique ID representing each individual client.
    pub uid: String,
    /// Perceived latency across all requests made by this client.
    pub latency: f64,
    /// Notifies the monitoring system that the running benchmark has finished.
    pub finish: bool,
    /// The perceived throughput across all keys.
    pub throughput: f64,
    /// Set during the benchmark warm-up phase to tell the monitoring system that
    /// it should ignore policy decisions.
    pub warmup: bool,
    /// Perceived latencies for individual keys.
    pub key_latency: Vec<KeyLatency>,
}

/// Observed latency measurements for individual keys.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyLatency {
    /// The key for which latency is being reported.
    pub key: ClientKey,
    /// The observed latency for this key.
    pub latency: f64,
}

/// A message to capture the periodic reporting of each server thread's local
/// statistics; these are aggregated by the monioring system.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ServerThreadStatistics {
    /// What percentage of the server thread's storage capacity is being consumed.
    pub storage_consumption: u64,
    /// What percentage of the server thread's compute capacity is being consumed.
    pub occupancy: f64,
    /// The server thread's reporting epoch.
    pub epoch: usize,
    /// How many key accesses were serviced during this epoch.
    pub access_count: usize,
}
