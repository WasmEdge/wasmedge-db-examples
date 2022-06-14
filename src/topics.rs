//! Defines the zenoh topic paths that should be used for messages.
//!
//! Allows to address specific threads of specific nodes.

use std::convert::TryInto;

// The topic on which clients send key address requests to routing nodes.
const KEY_ADDRESS_TOPIC: &str = "key_address";

// The topic on which clients receive responses from the KVS.
const USER_RESPONSE_TOPIC: &str = "user_response";

// The topic on which clients receive responses from the routing tier.
const USER_KEY_ADDRESS_TOPIC: &str = "user_key_address";

// The topic on which cache nodes listen for updates from the KVS.
const CACHE_UPDATE_TOPIC: &str = "cache_update";

// The topic on which KVS servers listen for new node announcments.
const NODE_JOIN_TOPIC: &str = "node_join";

// The topic on which KVS servers listen for node departures.
const NODE_DEPART_TOPIC: &str = "node_depart";

// The topic on which KVS servers are asked to depart by the monitoring system.
const SELF_DEPART_TOPIC: &str = "self_depart";

// The topic on which KVS servers listen for replication factor responses.
const SERVER_REPLICATION_RESPONSE_TOPIC: &str = "server_replication_response";

// The topic on which KVS servers listen for requests for data.
const KEY_REQUEST_TOPIC: &str = "key_request";

// The topic on which KVS servers listen for gossip from other KVS nodes.
const GOSSIP_TOPIC: &str = "gossip";

// The topic on which KVS servers listen for a replication factor change from
// the monitoring system.
const SERVER_REPLICATION_CHANGE_TOPIC: &str = "server_replication_change";

// The topic on which KVS servers listen for responses to a request for listing
// the keys cached at a function node.
const CACHE_IP_RESPONSE_TOPIC: &str = "cache_ip_response";

// The topic on which KVS servers listen for responses from management node to a
// request for the list of all existing function nodes.
const MANAGEMENT_NODE_RESPONSE_TOPIC: &str = "management_node_response";

const TCP_PORT_TOPIC: &str = "tcp_port";

// The topic on which routing servers listen for cluster membership requests.
const SEED_TOPIC: &str = "seed";
const ADVERTISEMENT_TOPIC: &str = "advertise-routing-node";

// The topic on which routing servers listen for cluster membership changes.
const ROUTING_NOTIFY_TOPIC: &str = "routing_notify";

// The topic on which routing servers listen for replication factor responses.
const ROUTING_REPLICATION_RESPONSE_TOPIC: &str = "routing_replication_response";

// The topic on which routing servers listen for replication factor change
// announcements from the monitoring system.
const ROUTING_REPLICATION_CHANGE_TOPIC: &str = "routing_replication_change";

// The topic on which monitoring threads listen for KVS responses when
// retrieving metadata.
const MONITORING_RESPONSE_TOPIC: &str = "monitoring_response";

// The topic on which the monitoring system waits for a response from KVS nodes
// after they have finished departing.
const DEPART_DONE_TOPIC: &str = "depart_done";

// The topic on which the monitoring system listens for cluster membership
// changes.
const MONITORING_NOTIFY_TOPIC: &str = "monitoring_notify";

// The topic on which the monitoring nodes listens for performance feedback from
// clients.
const FEEDBACK_REPORT_TOPIC: &str = "feedback_report";

// The topic on which benchmark nodes listen for triggers.
const BENCHMARK_COMMAND_TOPIC: &str = "benchmark_command";

/// Provides the topic paths for addressing a specific thread of a specific _KVS_ node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KvsThread {
    /// The ID of the addressed KVS node.
    pub node_id: String,
    /// The ID of the addressed thread on the KVS node.
    pub thread_id: u32,
}

impl KvsThread {
    /// Address the given thread on the given node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// When nodes join the cluster, they send a [`Join`][crate::messages::Join] message on this
    /// topic to relevant nodes.
    pub fn node_join_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, NODE_JOIN_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// After nodes departed the cluster, a [`Departed`][crate::messages::Departed]
    /// message is sent on this topic to relevant nodes.
    pub fn node_depart_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, NODE_DEPART_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Topic for notifying a node thread that it itself should leave.
    ///
    /// The messages that are sent on this topic are [`SelfDepart`][crate::messages::SelfDepart]
    /// messages.
    pub fn self_depart_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, SELF_DEPART_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The topic on which [`Request`][crate::messages::Request] messages are sent.
    pub fn request_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, KEY_REQUEST_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Topic on which gossip messages are sent.
    ///
    /// Gossip messages are of type [`Request`][crate::messages::Request].
    pub fn gossip_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, GOSSIP_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Used to notify KVS threads about replication factor changes.
    ///
    /// The messages sent on this topic are
    /// [`ReplicationFactorUpdate`](crate::messages::replication_factor::ReplicationFactorUpdate)
    /// messages.
    pub fn replication_change_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, SERVER_REPLICATION_CHANGE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The topic on which responses to replication requests are sent.
    ///
    /// The messages sent on this topic are [`Response`][crate::messages::Response] messages.
    pub fn replication_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, SERVER_REPLICATION_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Used for cached keys response messages, not fully implemented yet.
    pub fn cache_ip_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, CACHE_IP_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Used for response messages from the management node, not fully implemented yet.
    pub fn management_node_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, MANAGEMENT_NODE_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }
}

/// Provides the topic paths for addressing a specific thread of a specific _client_ node.
#[derive(Debug, Clone)]
pub struct ClientThread {
    /// The node ID of the client node.
    pub node_id: String,
    /// The ID of the addressed thread.
    pub thread_id: u32,
}

impl ClientThread {
    /// Address the given thread of the given client node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// The topic on which [`Response`][crate::messages::Response] messages should be sent in
    /// reply to requests.
    ///
    /// Clients send [`Request`][crate::messages::Request] messages to KVS nodes and pass
    /// this topic as reply topic.
    pub fn response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, USER_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The topic on which [`AddressResponse`][crate::messages::AddressResponse] messages should
    /// be sent in reply to address requests.
    ///
    /// Clients send [`AddressRequest`][crate::messages::AddressRequest] messages to routing
    /// nodes and pass this topic as reply topic.
    pub fn address_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, USER_KEY_ADDRESS_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }
}

/// Provides the topic paths for addressing a specific thread of a specific _routing_ node.
///
/// Each KVS has a configured routing node that it should address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutingThread {
    /// The ID if the routing node.
    pub node_id: String,
    /// The addressed thread ID of the routing node.
    pub thread_id: u32,
}

impl RoutingThread {
    /// KVS node threads send a `"join"` request on this topic on startup.
    ///
    /// The routing node should reply with a
    /// [`ClusterMembership`][crate::messages::cluster_membership::ClusterMembership]
    /// message. Unlike most other messages in this crate, the `"join"` is sent as
    /// zenoh [`get`][zenoh::Workspace::get] requests with an immediate reply.
    pub fn seed_topic(prefix: &str) -> String {
        format!("{}/{}", prefix, SEED_TOPIC).try_into().unwrap()
    }

    /// Each routing node broadcasts its ID on this topic.
    pub fn advertisement_topic(prefix: &str) -> String {
        format!("{}/{}", prefix, ADVERTISEMENT_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Addresses the given thread on the given routing node.
    pub fn new(node_id: String, thread_id: u32) -> Self {
        Self { node_id, thread_id }
    }

    /// Topic used for creating a point-to-point connection to this routing thread.
    pub fn p2p_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}", prefix, self.node_id, self.thread_id)
            .try_into()
            .unwrap()
    }

    /// Nodes can request the public IP address of the routing node under this topic.
    pub fn tcp_addr_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, TCP_PORT_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Used to notify routing threads of node joins and departures.
    ///
    /// The messages sent on this topic are of type [`Notify`][crate::messages::Notify].
    pub fn notify_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, ROUTING_NOTIFY_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Client nodes request the KVS node responsible for a given key on this topic.
    ///
    /// The sent messages are of type [`AddressRequest`][crate::messages::AddressRequest].
    pub fn address_request_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, KEY_ADDRESS_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// Topic for notifying routing nodes of replication changes.
    ///
    /// The messages sent on this topic are of type
    /// [`ReplicationFactorUpdate`][crate::messages::replication_factor::ReplicationFactorUpdate].
    pub fn replication_change_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, ROUTING_REPLICATION_CHANGE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The topic on which responses to replication requests are sent.
    ///
    /// The messages sent on this topic are [`Response`][crate::messages::Response] messages.
    pub fn replication_response_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, ROUTING_REPLICATION_RESPONSE_TOPIC, self.thread_id
        )
        .try_into()
        .unwrap()
    }

    /// The routing node responds to "ping" messages sent on this topic.
    pub fn ping_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}/{}", prefix, self.node_id, "ping", self.thread_id)
            .try_into()
            .unwrap()
    }
}

/// Provides the topic paths for addressing a specific _monitoring_ node.
pub struct MonitoringThread {
    /// The ID of the monitoring node.
    pub node_id: String,
}

impl MonitoringThread {
    /// Addresses the given monitoring node.
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Monitoring nodes are informed about node joins and departures on this topic.
    ///
    /// Sent messages are of type [`Notify`][crate::messages::Notify].
    pub fn notify_topic(prefix: &str) -> String {
        format!("{}/{}", prefix, MONITORING_NOTIFY_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Monitoring nodes are informed about finished departures on this topic.
    ///
    /// Sent messages are of type [`Departed`][crate::messages::Departed].
    pub fn depart_done_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}", prefix, self.node_id, DEPART_DONE_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Monitoring nodes expect responses on this topic.
    ///
    /// Sent messages are of type [`Response`][crate::messages::Response].
    pub fn response_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}", prefix, self.node_id, MONITORING_RESPONSE_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Benchmarking nodes send [`UserFeedback`][crate::messages::user_feedback::UserFeedback]
    /// messages to the monitoring nodes on this topic.
    pub fn feedback_report_topic(prefix: &str) -> String {
        format!("{}/{}", prefix, FEEDBACK_REPORT_TOPIC)
            .try_into()
            .unwrap()
    }
}

/// Not used right now (we only send on this topic, but never subscribe to it)
#[doc(hidden)]
pub struct CacheThread {
    node_id: String,
    tid: u32,
}

#[allow(missing_docs)]
impl CacheThread {
    pub fn new(ip: String, tid: u32) -> Self {
        Self { node_id: ip, tid }
    }

    pub fn cache_update_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            prefix, self.node_id, CACHE_UPDATE_TOPIC, self.tid
        )
        .try_into()
        .unwrap()
    }
}

/// Addresses a management node, which is reponsible for booting up and shutting down nodes.
#[doc(hidden)]
pub struct ManagementThread {
    /// The id of the management node.
    pub node_id: String,
}

impl ManagementThread {
    const QUERY_FUNC_NODES_TOPIC: &'static str = "query_func_nodes";
    const ADD_NODES_TOPIC: &'static str = "add_nodes";
    const REMOVE_NODE_TOPIC: &'static str = "remove_node";

    pub fn singleton() -> Self {
        Self {
            node_id: "management".into(),
        }
    }

    /// Request additional nodes.
    ///
    /// The messages sent on this topic should be of type
    /// [`AddNodes`][crate::messages::management::AddNodes].
    pub fn add_nodes_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}", prefix, self.node_id, Self::ADD_NODES_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Tells the management node to shut down a node.
    ///
    /// The messages sent on this topic should be of type
    /// [`RemoveNode`][crate::messages::management::RemoveNode].
    pub fn remove_node_topic(&self, prefix: &str) -> String {
        format!("{}/{}/{}", prefix, self.node_id, Self::REMOVE_NODE_TOPIC)
            .try_into()
            .unwrap()
    }

    /// Topic on which
    /// [`FuncNodesQuery`][crate::messages::management::FuncNodesQuery]
    /// messages can be sent.
    pub fn query_func_nodes_topic(&self, prefix: &str) -> String {
        format!(
            "{}/{}/{}",
            prefix,
            self.node_id,
            Self::QUERY_FUNC_NODES_TOPIC
        )
        .try_into()
        .unwrap()
    }
}

/// Topic on which commands to benchmark nodes are sent.
pub fn benchmark_topic(thread_id: u32, zenoh_prefix: &str) -> String {
    format!(
        "{}/benchmark/{}/{}",
        zenoh_prefix, BENCHMARK_COMMAND_TOPIC, thread_id,
    )
    .try_into()
    .unwrap()
}
