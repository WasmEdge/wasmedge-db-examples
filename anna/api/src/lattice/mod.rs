//! Contains the [`Lattice`] trait and its implementations.
//!
//! The following base lattices are available:
//!
//! - **[`BoolLattice`]:** A boolean value that uses the logical OR operation for merges.
//! - **[`MaxLattice`]:** Defines the merge operation as the maximum of the two values.
//! - **[`SetLattice`]** and **[`OrderedSetLattice`]**: A set of items that uses the union
//!     operator for merging.
//! - **[`LastWriterWinsLattice`]:** Keeps track of the creation time of each value and chooses
//!     the newer value on merging.
//!
//! There are also compound lattices that can wrap other lattice values:
//!
//! - **[`MapLattice`]:** A hash map that stores lattice types. When merging two maps,
//!     conflicting values are resolved by applying their merge operator.
//!
//! By combining these lattice types, different levels of consistency can be achieved. For
//! example, the [`causal`] submodule contains lattice types to achieve causal consistency.

pub use self::{
    bool::BoolLattice, last_writer_wins::LastWriterWinsLattice, map::MapLattice, max::MaxLattice,
    ordered_set::OrderedSetLattice, set::SetLattice,
};

pub mod causal;
pub mod last_writer_wins;

mod bool;
mod map;
mod max;
mod ordered_set;
mod set;

/// Abstraction for a [_bounded join semilattice_](https://en.wikipedia.org/wiki/Semilattice),
/// which is the foundation of the coordination freedom of Anna.
///
/// A join semilattice is a set that has an unique supremum (least upper bound) operator `⊔`
/// for all pairs of values. The `⊔` operator must be
/// [commutative](https://en.wikipedia.org/wiki/Commutative),
/// [associative](https://en.wikipedia.org/wiki/Associative_property), and
/// [idempotent](https://en.wikipedia.org/wiki/Idempotence).
///
/// A common example for such a supremum operator `⊔` is the
/// [union operation](https://en.wikipedia.org/wiki/%E2%8B%83) `⋃` on sets.
///
/// The lattice properties are useful for a key value store since the supremum of a set of
/// values does not depend on the order they are merged together. Thus, we can merge key
/// updates in different orders across nodes and still reach the same end value on all of
/// them. This way, we can guarantee consistency across all replicas without any synchronization.
///
/// Since the consistency of the key value store depends on the guarantees of this trait, **all
/// implementations must fulfill all the join semilattice properties**. Instead of implementing
/// this trait for new types, it is often possible to instead compose the existing types
/// that implement `Lattice` into more complex types.
pub trait Lattice {
    /// The type that is stored in this lattice.
    type Element;

    /// Returns the current value stored in the lattice.
    fn reveal(&self) -> &Self::Element;

    /// Returns the current value stored in the lattice, taking ownership.
    fn into_revealed(self) -> Self::Element;

    /// Assigns a new value to the lattice without any merging.
    fn assign(&mut self, element: Self::Element);

    /// Updates the lattice value with the supremum of the current and given values.
    ///
    /// This implements the supremum operator `⊔` described above.
    fn merge_element(&mut self, element: &Self::Element);

    /// Updates the lattice value with the supremum of the current and given values.
    ///
    /// This is a convenience methd that reveals the value of `other` and then calls the
    /// [`merge_element`][Self::merge_element] method.
    fn merge(&mut self, other: &Self) {
        self.merge_element(other.reveal());
    }
}
