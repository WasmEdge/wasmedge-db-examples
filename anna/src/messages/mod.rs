//! Defines the message types that are sent between nodes and threads.

use crate::topics::KvsThread;

pub use self::{
    addr_request::AddressRequest,
    addr_response::{AddressResponse, KeyAddress},
    depart::{Departed, SelfDepart},
    join::Join,
    notify::Notify,
    request::Request,
    response::Response,
};
use std::{hash::Hash, net::SocketAddr};

mod addr_request;
mod addr_response;
mod depart;
mod join;
mod notify;

pub mod cluster_membership;
pub mod key_data;
pub mod management;
pub mod replication_factor;
pub mod request;
pub mod response;
pub mod user_feedback;

/// The message type that anna nodes send over TCP.
///
/// This type is only used for TCP messages. For messages sent over `zenoh`,
/// the wrapped inner types are used directly.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TcpMessage {
    /// Ping message to test latency.
    ///
    /// The receiver should respond with a `Pong` message.
    Ping {
        /// The payload that the receiver should respond with.
        payload: Vec<u8>,
    },
    /// Reply to a `Ping` message.
    Pong {
        /// Sends back the payload given in the `Ping` message.
        payload: Vec<u8>,
    },
    /// KVS nodes send this message to all routing nodes with public IPs after
    /// they join the cluster.
    ///
    /// Through this message, routing nodes have a direct TCP channel to each
    /// KVS node.
    TcpJoin {
        /// The ID and thread number of the joined KVS node.
        kvs_thread: KvsThread,
        /// The public socket address at which the joined KVS can be reached (if any).
        ///
        /// The routing nodes forward this address to client nodes, which can then
        /// open a direct TCP connection to the KVS node when they want to contact
        /// it, which is faster than the default `zenoh` connection.
        listen_socket: Option<SocketAddr>,
        // TODO: also send send private IP address
    },
    /// When a KVS node with a public IP joins, the routing node forwards its socket
    /// address to other KVS nodes using this message variant.
    ///
    /// KVS nodes react to this message by opening a TCP connection to the other
    /// node. This way, the communication between nodes can use the faster direct
    /// connection instead of going through `zenoh`.
    KvsSocket {
        /// The ID of the KVS thread that this message is about.
        kvs_thread: KvsThread,
        // TODO: send private IP address here
        /// The public socket address at which the `kvs_thread` can be reached.
        socket: SocketAddr,
    },
    /// A [`Notify`] message.
    Notify(Notify),
    /// An [`AddressRequest`] message.
    AddressRequest(AddressRequest),
    /// An [`AddressResponse`] message.
    AddressResponse(AddressResponse),
    /// A [`Request`] message.
    Request(Request),
    /// A [`Response`] message.
    Response(Response),
}

/// An enum representing all the tiers the system supports -- currently, a
/// memory tier and a disk-based tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Tier {
    /// The value for the memory tier.
    Memory,
    /// The value for the disk-based tier.
    Disk,
    /// The value for the routing tier.
    Routing,
}

/// Each routing node periodically broadcasts this message.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RoutingNodeAdvertisement {
    /// The node ID of the sending routing node.
    pub node_id: String,
}
