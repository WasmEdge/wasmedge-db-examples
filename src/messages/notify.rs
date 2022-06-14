//! Private module containing the [`Notify`] type.

use super::{Departed, Join};

/// Notifies the routing node about node joins and departures.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Notify {
    /// A node joined the cluster.
    Join(Join),
    /// A node left the cluster.
    Depart(Departed),
}
