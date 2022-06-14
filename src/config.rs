//! Types for parsing anna configuration files.
//!
//! The top level config type is [`Config`].

use serde::{Deserialize, Serialize};

/// The top level config type.
///
/// This type can be read and written to config files using the [`serde::Serialize`] and
/// [`serde::Deserialize`] implementations.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Defines the number of threads that should be used per node.
    pub threads: Threads,
    /// Defines the replication factors for different tiers.
    pub replication: Replication,
    /// Defines the memory capacity for each tier.
    pub capacities: Capacities,
    /// Controls which policies of the monitoring should be enabled.
    pub policy: Policy,
}

/// Specifies the number of threads that should be spawned for nodes.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct Threads {
    /// The number of threads that should be spawned for KVS nodes on the memory tier.
    pub memory: u32,
    /// The number of threads that should be spawned for KVS nodes on the disk tier.
    pub ebs: u32,
    /// The number of threads that should be spawned for routing nodes.
    pub routing: u32,
    /// The number of threads that should be spawned for benchmark nodes.
    pub benchmark: u32,
}

/// Specifies the replication factors that should be used for nodes.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct Replication {
    /// The replication factor for KVS nodes on the memory tier.
    pub memory: usize,
    /// The replication factor for KVS nodes on the disk tier.
    pub ebs: usize,
    /// The local replication factor.
    pub local: usize,
    /// The minimum replication factor.
    pub minimum: usize,
}

/// Defines the capacity of KVS nodes depending on the [`Tier`][crate::messages::Tier].
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct Capacities {
    /// Capacity of memory nodes.
    pub memory_cap: u64,
    /// Capacity of disk nodes.
    pub ebs_cap: u64,
}

/// The `anna` KVS supports different policies for dynamically optimizing the key distribution
/// and cluster layout at runtime.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct Policy {
    /// Adds and remove nodes dynamically if needed, e.g. on insufficient storage capacity.
    pub elasticity: bool,
    /// Automatically promote/demote keys across storage tiers based on their access frequency.
    pub tiering: bool,
    /// Automatically increase the number of in-memory replicas for hot keys.
    pub selective_rep: bool,
}
