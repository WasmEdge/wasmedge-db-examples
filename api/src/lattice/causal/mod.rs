//! Contains the [`SingleKeyCausalLattice`] and [`MultiKeyCausalLattice`] that provide causal
//! consistency.

pub use self::{
    multi_key_causal::{MultiKeyCausalLattice, MultiKeyCausalPayload},
    single_key_causal::{SingleKeyCausalLattice, VectorClockValuePair},
};
use super::{MapLattice, MaxLattice};

mod multi_key_causal;
mod single_key_causal;

/// A [vector clock](https://en.wikipedia.org/wiki/Vector_clock) allows to determine the partial
/// ordering of events in a distributed system.
///
/// This vector clock type uses a [`MapLattice`] to store the logical clock per node. By using a
/// [`MaxLattice`] as value type, the vector clock is monotonically updated on merging.
///
/// This type alias is used by both the [`SingleKeyCausalLattice`] and [`MultiKeyCausalLattice`].
pub type VectorClock = MapLattice<String, MaxLattice<usize>>;
