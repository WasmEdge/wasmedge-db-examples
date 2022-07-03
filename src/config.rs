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
}

/// Specifies the number of threads that should be spawned for nodes.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, Copy)]
#[serde(rename_all = "kebab-case")]
pub struct Threads {
    /// The number of threads that should be spawned for routing nodes.
    pub routing: u32,
}
