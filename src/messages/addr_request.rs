//! Private module containing the [`AddressRequest`] type.

use crate::ClientKey;

/// A request to the routing tier to retrieve server addresses corresponding to
/// individual keys.
///
/// The routing tier will respond with a [`AddressResponse`][super::AddressResponse].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AddressRequest {
    /// The topic at which the client will await a response.
    pub response_address: String,
    /// The names of the requested keys.
    pub keys: Vec<ClientKey>,
    /// A unique ID used by the client to match asynchronous requests with
    /// responses.
    pub request_id: String,
}
