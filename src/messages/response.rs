//! Provides the main [`Response`] struct and related types.

use crate::{store::LatticeValue, AnnaError, Key};

/// A response to a [`Request`][super::Request].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Response {
    /// The request_id specified in the corresponding KeyRequest. Used to
    /// associate asynchornous requests and responses.
    pub response_id: Option<String>,
    /// The type of response being sent back to the client (see RequestType).
    pub ty: ResponseType,
    /// Any errors associated with the whole request. Individual tuple errors are
    /// captured in the corresponding KeyTuple. This will only be set if the whole
    /// request times out.
    pub error: Result<(), AnnaError>,
    /// The individual response pairs associated with this request. There is a
    /// 1-to-1 mapping between these and the KeyTuples in the corresponding
    /// KeyRequest.
    pub tuples: Vec<ResponseTuple>,
}

/// Specifies the type of operation that we executed.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum ResponseType {
    /// Response to a request to retrieve data from the KVS.
    Get,
    /// Response to a request to put data into the KVS.
    Put,
}

/// A protobuf to represent an individual key, both for requests and responses.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResponseTuple {
    /// The key name for this response.
    pub key: Key,
    /// The lattice value for this key.
    pub lattice: Option<LatticeValue>,
    /// The error type specified by the server (see AnnaError).
    pub error: Option<AnnaError>,
    /// A boolean set by the server if the client's address_cache_size does not
    /// match the metadata stored by the server.
    pub invalidate: bool,
}
