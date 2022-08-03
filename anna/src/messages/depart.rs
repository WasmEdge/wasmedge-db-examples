//! Private module containing the [`SelfDepart`] and [`Departed`] types.

use super::Tier;

/// Signals that a node left the cluster.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Departed {
    /// The tier of the departed node.
    pub tier: Tier,
    /// The ID of the departed node.
    pub node_id: String,
}

/// The receiving node should leave the cluster.
///
/// It should send a [`Departed`] message as an acknowledge message to the given `response_topic`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SelfDepart {
    /// The topic on which the `Departed` message should be sent.
    pub response_topic: String,
}
