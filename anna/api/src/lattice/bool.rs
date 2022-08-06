//! Contains the [`BoolLattice`] type.

use super::Lattice;

/// Defines a lattice on the set `{false, true}` with the
/// [boolean OR operation](https://en.wikipedia.org/wiki/Boolean_algebra#Basic_operations)
/// as supremum operator.
///
/// ## Example
///
/// ```
/// use anna_api::lattice::{BoolLattice, Lattice};
///
/// // initialize a new Lattice value with `false`
/// let mut lattice = BoolLattice::new(false);
/// assert_eq!(lattice.reveal(), &false);
///
/// // merging `false` does not change anyhing since `false | false` is `false`.
/// lattice.merge_element(&false);
/// assert_eq!(lattice.reveal(), &false);
///
/// // merging `true` the value to `false | true`, which results in `true`.
/// lattice.merge_element(&true);
/// assert_eq!(lattice.reveal(), &true);
///
/// // further merge operations don't change anything since `true | x` is always `true`
/// lattice.merge_element(&false);
/// assert_eq!(lattice.reveal(), &true);
/// ```
#[derive(Default)]
pub struct BoolLattice {
    /// The boolean value stored in this lattice.
    element: bool,
}

impl Lattice for BoolLattice {
    type Element = bool;

    fn reveal(&self) -> &bool {
        &self.element
    }

    fn into_revealed(self) -> bool {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, element: &bool) {
        self.element |= element;
    }
}

impl BoolLattice {
    /// Creates a new lattice from the given value.
    pub fn new(element: bool) -> Self {
        BoolLattice { element }
    }
}

#[cfg(test)]
mod tests {
    // the data type is bool, so `assert_eq!(x, false)` is clearer than `assert!(!x)`
    #![allow(clippy::bool_assert_comparison)]

    use super::*;

    #[test]
    fn assign() {
        let mut bl = BoolLattice::default();
        assert_eq!(*bl.reveal(), false);
        bl.assign(true);
        assert_eq!(*bl.reveal(), true);
        bl.assign(false);
        assert_eq!(*bl.reveal(), false);
    }

    #[test]
    fn merge_by_value() {
        let mut bl = BoolLattice::default();
        assert_eq!(*bl.reveal(), false);
        bl.merge_element(&false);
        assert_eq!(*bl.reveal(), false);
        bl.merge_element(&true);
        assert_eq!(*bl.reveal(), true);
        bl.merge_element(&false);
        assert_eq!(*bl.reveal(), true);
    }

    #[test]
    fn merge_by_lattice() {
        let mut bl = BoolLattice::default();
        assert_eq!(*bl.reveal(), false);
        bl.merge(&BoolLattice { element: false });
        assert_eq!(*bl.reveal(), false);
        bl.merge(&BoolLattice { element: true });
        assert_eq!(*bl.reveal(), true);
        bl.merge(&BoolLattice { element: false });
        assert_eq!(*bl.reveal(), true);
    }
}
