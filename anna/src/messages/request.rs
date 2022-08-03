//! Provides the main [`Request`] struct and related types.

use super::response::{Response, ResponseType};
use crate::{store::LatticeValue, ClientKey, Key};
use std::collections::HashMap;

/// An individual GET or PUT request; each request can batch multiple keys.
///
/// The target node responds with a [`Response`][super::Response].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Request {
    /// A client-specific ID used to match asynchronous requests with responses.
    pub request_id: Option<String>,
    /// The zenoh topic at which the client is waiting for the server's response.
    pub response_address: Option<String>,
    /// The number of server addresses the client is aware of for a particular
    /// key; used for DHT membership change optimization.
    pub address_cache_size: HashMap<ClientKey, usize>,
    /// The type and data of this request.
    pub request: RequestData,
}

impl Request {
    /// Constructs a new [`Response`] for the request.
    ///
    /// Sets [`response_id`][Response::response_id] and `ty`[Response::ty] fields accordingly.
    /// The [`error`][Response::error] field is initialized with [`Ok(())`][Result::ok] and
    /// the [`tuples`][Response::tuples] field with an empty list.
    pub fn new_response(&self) -> Response {
        Response {
            response_id: self.request_id.clone(),
            ty: self.request.ty(),
            tuples: Default::default(),
            error: Ok(()),
        }
    }
}

/// Specifies the request type and associated data.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum RequestData {
    /// Request the stored values for a set of keys.
    Get {
        /// The list of keys that we want to get the values for.
        keys: Vec<Key>,
    },
    /// Performs the given updates in the key value store.
    Put {
        /// A list of updates batched in this request.
        tuples: Vec<PutTuple>,
    },
}

impl RequestData {
    /// Splits the request into a list of operations.
    ///
    /// For GET requests, this returns a list of [`KeyOperation::Get`] variants. For PUT
    /// requests, it returns a list of [`KeyOperation::Put`] variants.
    pub fn into_tuples(self) -> Vec<KeyOperation> {
        match self {
            RequestData::Get { keys: tuples } => {
                tuples.into_iter().map(KeyOperation::Get).collect()
            }
            RequestData::Put { tuples } => tuples.into_iter().map(KeyOperation::Put).collect(),
        }
    }

    /// Returns the suitable [`ResponseType`] for this request.
    pub fn ty(&self) -> ResponseType {
        match self {
            RequestData::Get { .. } => ResponseType::Get,
            RequestData::Put { .. } => ResponseType::Put,
        }
    }
}

/// Describes an assign operation on a specific key.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PutTuple {
    /// The key that should be updated.
    pub key: Key,
    /// The new value that should be merged into the current one.
    pub value: LatticeValue,
}

/// Abstraction for a single key operation.
#[derive(Debug)]
pub enum KeyOperation {
    /// Get the value of a key.
    Get(Key),
    /// Assign a new value to a key.
    Put(PutTuple),
}

impl KeyOperation {
    /// Returns the key that this operation reads/writes.
    pub fn key(&self) -> &Key {
        match self {
            KeyOperation::Get(key) => key,
            KeyOperation::Put(t) => &t.key,
        }
    }

    /// Returns the suitable [`ResponseType`] for the operation.
    pub fn response_ty(&self) -> ResponseType {
        match self {
            KeyOperation::Get(_) => ResponseType::Get,
            KeyOperation::Put(_) => ResponseType::Put,
        }
    }

    /// Returns the associated [`LatticeValue`] if this is a PUT operation.
    ///
    /// Returns [`None`][Option::None] if this is a GET operation.
    pub fn into_value(self) -> Option<LatticeValue> {
        match self {
            KeyOperation::Get(_) => None,
            KeyOperation::Put(t) => Some(t.value),
        }
    }
}
