//! Statistics data about keys.

use crate::{ClientKey, Key};

/// A message to capture the access frequencies of individual keys for a
/// particular server thread.
#[derive(Default, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyAccessData {
    /// A list of all the key access frequencies tracked during this epoch.
    pub keys: Vec<KeyCount>,
}

/// A mapping from an individual key to its access count.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyCount {
    /// The key being tracked.
    pub key: Key,
    /// The number of times this key was accessed during this epoch.
    pub access_count: usize,
}

/// A message to track metadata about how large each key in the system is.
#[derive(Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeySizeData {
    /// The list of key size metadata tuples being reported.
    pub key_sizes: Vec<KeySize>,
}

/// The size metadata for an individual key.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeySize {
    /// The key for which size metadata is being reported.
    pub key: ClientKey,
    /// The estimated size of the associated value.
    pub size: usize,
}
