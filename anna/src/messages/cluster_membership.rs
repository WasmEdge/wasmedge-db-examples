//! Information about the node cluster, i.e. which nodes exist for which [`Tier`].

use super::Tier;

/// A message to track which physical servers are a part of which Anna
/// membership (memory, disk) tier.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClusterInfo {
    /// The set of all tiers in the system.
    pub tiers: Vec<TierMembership>,
    /// List of all routing nodes in the cluster.
    pub routing_node_ids: Vec<String>,
}

/// The representation the servers comprising an individual tier.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TierMembership {
    /// The Tier represented by this message -- either MEMORY or DISK.
    pub tier_id: Tier,
    /// The list of server node ids in this tier.
    pub servers: Vec<String>,
}
