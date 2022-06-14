//! Communication with the management node.

use super::Tier;
use std::collections::HashSet;

/// Adds the given number of nodes to the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AddNodes {
    /// The number of new nodes that should be added.
    pub number: usize,
    /// The tier in which the new nodes should be added.
    pub tier: Tier,
}

/// Shuts down the given node that is no longer part of the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RemoveNode {
    /// The ID of the node that departed from the cluster.
    pub departed_node_id: String,
}

/// Query a [`NodeSet`] from the management node.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FuncNodesQuery {
    /// The managment node should reply on this topic.
    pub response_topic: String,
}

/// A set of node IDs.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeSet {
    /// The set of node IDs.
    pub nodes: HashSet<String>,
}
