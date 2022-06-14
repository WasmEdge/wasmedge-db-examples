//! Messages related to replication of keys.

use crate::ClientKey;

/// A message that captures the replication factor for an individual key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationFactor {
    /// The name of the key whose replication factor is being changed.
    pub key: ClientKey,
    /// A set of mappings from individual tiers (MEMORY, DISK -- see Tier enum)
    /// to the cross-machine replication factor at that tier.
    pub global: Vec<ReplicationValue>,
    /// A set of mappings from individual tiers (MEMORY, DISK -- see Tier enum)
    /// to the intra-machine replication factor at that tier.
    pub local: Vec<ReplicationValue>,
}

/// A message representing the replication level for a single key at a
/// single tier.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationValue {
    /// The tier represented by this message.
    pub tier: super::Tier,
    /// The replication level at this particular tier for this particular key.
    pub value: usize,
}

/// A message to propagate changes to a set of keys' replication factors.
#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationFactorUpdate {
    /// The set of replication factor updates being sent.
    pub updates: Vec<ReplicationFactor>,
}
