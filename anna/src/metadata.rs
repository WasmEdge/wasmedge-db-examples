//! Provides the [`MetadataKey`] type that can be used to address thread or key-specific metadata.
//!
//! Also contains the [`TierMetadata`] struct that describes tier-related metadata.

use crate::{messages::Tier, topics::KvsThread, ClientKey, Key};
use eyre::anyhow;

/// Used to request and update metadata between nodes.
#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Serialize, serde::Deserialize)]
pub enum MetadataKey {
    /// Key under which the the replication metadata of a [`ClientKey`] is stored.
    Replication {
        /// The client key that whose replication is requested/updated.
        key: ClientKey,
    },
    /// Metadata about threads of KVS nodes is stored under this key.
    KvsThread {
        /// The tier that the node belongs to.
        tier: Tier,
        /// The KVS node and thread that the metadata is about.
        kvs_thread: KvsThread,
        /// The kind of metadata that is requested/updated.
        kind: KvsMetadataKind,
    },
    /// Only partially used right now.
    CacheId {
        #[allow(missing_docs)]
        key: String,
    },
}

impl std::convert::TryFrom<Key> for MetadataKey {
    type Error = eyre::Error;

    fn try_from(value: Key) -> Result<Self, Self::Error> {
        match value {
            Key::Client(_) => Err(anyhow!("key is a client key instead of a metadata key")),
            Key::Metadata(key) => Ok(key),
        }
    }
}

/// Describes the kind of metadata of a KVS node thread.
#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Serialize, serde::Deserialize)]
pub enum KvsMetadataKind {
    /// Used to address the thread's
    /// [`ServerThreadStatistics`][crate::messages::user_feedback::ServerThreadStatistics].
    ServerStats,
    /// Stores the [`KeyAccessData`][crate::messages::key_data::KeyAccessData] of the node thread.
    KeyAccess,
    /// Stores the [`KeySizeData`][crate::messages::key_data::KeySizeData] of the node thread.
    KeySize,
}

/// Describes per-[`Tier`] metadata.
#[derive(Debug, Clone)]
pub struct TierMetadata {
    /// The number of threads configured for the tier.
    pub thread_number: u32,
    /// The default replication factor configured for the tier.
    pub default_replication: usize,
    /// The configured capacity of nodes of this tier.
    pub node_capacity: u64,
}
