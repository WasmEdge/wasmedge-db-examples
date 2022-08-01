//! Types for parsing anna configuration files.
//!
//! The top level config type is [`Config`].

use std::net::IpAddr;

use argh::FromArgs;
use serde::{Deserialize, Serialize};

/// The top level config type.
///
/// This type can be read and written to config files using the [`serde::Serialize`] and
/// [`serde::Deserialize`] implementations.
#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize, FromArgs)]
#[argh(description = "Rusty anna client")]
pub struct Config {
    /// ip address of routing node.
    #[argh(option, short = 'h')]
    pub routing_ip: IpAddr,

    /// tcp port base of routing node.
    #[argh(option, short = 'p')]
    pub routing_port_base: u16,

    /// number of threads used for routing.
    #[argh(option, short = 'r')]
    pub routing_threads: u32,
}
