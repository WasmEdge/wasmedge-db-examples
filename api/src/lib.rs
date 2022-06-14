use eyre::anyhow;
use lattice::{
    causal::{MultiKeyCausalLattice, SingleKeyCausalLattice},
    LastWriterWinsLattice, OrderedSetLattice, SetLattice,
};
use std::{error::Error, fmt::Display, sync::Arc};

use crate::lattice::Lattice;

pub mod lattice;

/// A string-based key type used to store user-supplied data.
///
/// We use an [`Arc`]-wrapped [`String`] because keys often get cloned. For bare strings, this
/// would require a reallocation, but with the `Arc` wrapper only reference counter is
/// incremented.
#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientKey(Arc<String>);

impl std::ops::Deref for ClientKey {
    type Target = Arc<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ClientKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<Arc<String>> for ClientKey {
    fn from(k: Arc<String>) -> Self {
        Self(k)
    }
}

impl From<String> for ClientKey {
    fn from(k: String) -> Self {
        Self::from(Arc::new(k))
    }
}

impl From<&str> for ClientKey {
    fn from(k: &str) -> Self {
        Self::from(k.to_owned())
    }
}

/// Describes a value stored in the key value store.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LatticeValue {
    /// Last-writer wins lattice
    Lww(LastWriterWinsLattice<Vec<u8>>),
    /// Unordered set lattice
    Set(SetLattice<Vec<u8>>),
    /// Ordered-set lattice
    OrderedSet(OrderedSetLattice<Vec<u8>>),
    /// Single-key causal lattice
    SingleCausal(SingleKeyCausalLattice<SetLattice<Vec<u8>>>),
    /// Multi-key causal lattice
    MultiCausal(MultiKeyCausalLattice<SetLattice<Vec<u8>>>),
}

impl LatticeValue {
    /// Tries to cast the value to an [`LastWriterWinsLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_lww(&self) -> eyre::Result<&LastWriterWinsLattice<Vec<u8>>> {
        match self {
            Self::Lww(val) => Ok(val),
            other => Err(anyhow!("expected Lww lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`LastWriterWinsLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_lww(self) -> eyre::Result<LastWriterWinsLattice<Vec<u8>>> {
        match self {
            Self::Lww(val) => Ok(val),
            other => Err(anyhow!("expected Lww lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SetLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_set(&self) -> eyre::Result<&SetLattice<Vec<u8>>> {
        match self {
            Self::Set(val) => Ok(val),
            other => Err(anyhow!("expected Set lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SetLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_set(self) -> eyre::Result<SetLattice<Vec<u8>>> {
        match self {
            Self::Set(val) => Ok(val),
            other => Err(anyhow!("expected Set lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`OrderedSetLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_ordered_set(&self) -> eyre::Result<&OrderedSetLattice<Vec<u8>>> {
        match self {
            Self::OrderedSet(val) => Ok(val),
            other => Err(anyhow!("expected OrderedSet lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to an [`OrderedSetLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_ordered_set(self) -> eyre::Result<OrderedSetLattice<Vec<u8>>> {
        match self {
            Self::OrderedSet(val) => Ok(val),
            other => Err(anyhow!("expected OrderedSet lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SingleKeyCausalLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_single_causal(&self) -> eyre::Result<&SingleKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::SingleCausal(val) => Ok(val),
            other => Err(anyhow!("expected SingleCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`SingleKeyCausalLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_single_causal(self) -> eyre::Result<SingleKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::SingleCausal(val) => Ok(val),
            other => Err(anyhow!("expected SingleCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`MultiKeyCausalLattice`].
    ///
    /// Errors if the value is of a different type.
    pub fn as_multi_causal(&self) -> eyre::Result<&MultiKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::MultiCausal(val) => Ok(val),
            other => Err(anyhow!("expected MultiCausal lattice, got `{:?}`", other)),
        }
    }

    /// Tries to cast the value to a [`MultiKeyCausalLattice`], taking ownership.
    ///
    /// Errors if the value is of a different type.
    pub fn into_multi_causal(self) -> eyre::Result<MultiKeyCausalLattice<SetLattice<Vec<u8>>>> {
        match self {
            Self::MultiCausal(val) => Ok(val),
            other => Err(anyhow!("expected MultiCausal lattice, got `{:?}`", other)),
        }
    }

    /// Merges the given value into `self` if both values are of the same lattice type.
    ///
    /// If the given value is of a different lattice type than `self`, an `AnnaError::Lattice`
    /// is returned.
    pub fn try_merge(&mut self, other: &LatticeValue) -> Result<(), AnnaError> {
        match (self, other) {
            (LatticeValue::Lww(s), LatticeValue::Lww(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::Set(s), LatticeValue::Set(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::OrderedSet(s), LatticeValue::OrderedSet(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::SingleCausal(s), LatticeValue::SingleCausal(other)) => {
                s.merge(other);
                Ok(())
            }
            (LatticeValue::MultiCausal(s), LatticeValue::MultiCausal(other)) => {
                s.merge(other);
                Ok(())
            }
            (x, y) if x.ty() == y.ty() => unreachable!(),
            _ => Err(AnnaError::Lattice),
        }
    }

    /// Returns the lattice type of the value.
    pub fn ty(&self) -> LatticeType {
        match self {
            LatticeValue::Lww(_) => LatticeType::Lww,
            LatticeValue::Set(_) => LatticeType::Set,
            LatticeValue::OrderedSet(_) => LatticeType::OrderedSet,
            LatticeValue::SingleCausal(_) => LatticeType::SingleCausal,
            LatticeValue::MultiCausal(_) => LatticeType::MultiCausal,
        }
    }
}

impl From<MultiKeyCausalLattice<SetLattice<Vec<u8>>>> for LatticeValue {
    fn from(v: MultiKeyCausalLattice<SetLattice<Vec<u8>>>) -> Self {
        Self::MultiCausal(v)
    }
}

impl From<SingleKeyCausalLattice<SetLattice<Vec<u8>>>> for LatticeValue {
    fn from(v: SingleKeyCausalLattice<SetLattice<Vec<u8>>>) -> Self {
        Self::SingleCausal(v)
    }
}

impl From<OrderedSetLattice<Vec<u8>>> for LatticeValue {
    fn from(v: OrderedSetLattice<Vec<u8>>) -> Self {
        Self::OrderedSet(v)
    }
}

impl From<SetLattice<Vec<u8>>> for LatticeValue {
    fn from(v: SetLattice<Vec<u8>>) -> Self {
        Self::Set(v)
    }
}

impl From<LastWriterWinsLattice<Vec<u8>>> for LatticeValue {
    fn from(val: LastWriterWinsLattice<Vec<u8>>) -> Self {
        Self::Lww(val)
    }
}

/// Defines the different types of lattices that we support.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum LatticeType {
    /// Last-writer wins lattice
    Lww,
    /// Unordered set lattice
    Set,
    /// Single-key causal lattice
    SingleCausal,
    /// Multi-key causal lattice
    MultiCausal,
    /// Ordered-set lattice
    OrderedSet,
    /// Priority lattice
    Priority,
}

/// Used to signal errors in messages.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum AnnaError {
    /// The requested key does not exist.
    KeyDoesNotExist,
    /// The request was sent to the wrong thread, which is not responsible for the
    /// key.
    WrongThread,
    /// The request timed out.
    Timeout,
    /// The lattice type was not correctly specified or conflicted with an
    /// existing key.
    Lattice,
    /// This error is returned by the routing tier if no servers are in the
    /// cluster.
    NoServers,
    /// Failed to serialize a message.
    Serialize,
}

impl Display for AnnaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyDoesNotExist => write!(f, "The requested key does not exist."),
            Self::WrongThread => write!(
                f,
                "The request was sent to the wrong thread, which is not responsible for the key."
            ),
            Self::Timeout => write!(f, "The request timed out."),

            Self::Lattice => write!(
                f,
                "The lattice type was not correctly specified or conflicted with an existing key."
            ),
            Self::NoServers => write!(
                f,
                "This error is returned by the routing tier if no servers are in the cluster."
            ),
            Self::Serialize => write!(f, "Serialization error."),
        }
    }
}

impl Error for AnnaError {}

impl From<serde_json::Error> for AnnaError {
    fn from(_: serde_json::Error) -> Self {
        Self::Serialize
    }
}
