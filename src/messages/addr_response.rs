//! Private module containing the [`AddressResponse`] and [`KeyAddress`] types.

use crate::{topics::KvsThread, AnnaError, ClientKey};
use std::net::SocketAddr;

/// A 1-to-1 response from the routing tier for individual [`KeyAddressRequest`][super::AddressRequest]s.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AddressResponse {
    /// A batch of responses for individual keys.
    pub addresses: Vec<KeyAddress>,
    /// An error reported by the routing tier. This should only ever be a timeout.
    pub error: Option<AnnaError>,
    /// A unique ID used by the client to match asynchronous requests with
    /// responses.
    pub response_id: String,
    /// Contains the relevant public socket addresses that are known to the routing node.
    ///
    /// Only the socket addresses of the `KvsThread`s listed in the `addresses` field
    /// are reported (only if they're known).
    pub tcp_sockets: Vec<(KvsThread, SocketAddr)>,
}

/// Specifies the responsible server nodes for a key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyAddress {
    /// The key for which the `nodes` are responsible.
    pub key: ClientKey,
    /// The kvs nodes that are responsible for the `key`.
    pub nodes: Vec<KvsThread>,
}
