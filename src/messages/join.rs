//! Private module containing the [`Join`] type.

use super::Tier;

/// Signals that a node joined the cluster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Join {
    /// The tier of the newly joined node.
    pub tier: Tier,
    /// The ID of the joined node.
    pub node_id: String,
    /// Counter to signal that a node is rejoining.
    pub join_count: u32,
}
