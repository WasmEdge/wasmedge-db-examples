use super::Lattice;
use std::ops;

/// [`Lattice`] implementation that merges elements by taking their maximum.
///
/// ## Examples
///
/// The merge operation results in the maximum:
///
/// ```
/// use anna_api::lattice::{Lattice, MaxLattice};
///
/// let mut lattice = MaxLattice::new(4);
/// assert_eq!(lattice.reveal(), &4);
///
/// lattice.merge_element(&6);
/// assert_eq!(lattice.reveal(), &6);
///
/// lattice.merge_element(&5);
/// assert_eq!(lattice.reveal(), &6);
/// ```
///
/// The `MaxLattice` type implements the [`Add`][ops::Add] and [`Sub`][ops::Sub] traits, so it
/// can be manipulated through the `+` and `-` operators:
///
/// ```
/// use anna_api::lattice::{Lattice, MaxLattice};
///
/// let mut lattice = MaxLattice::new(48);
/// assert_eq!(lattice.reveal(), &48);
///
/// assert_eq!(lattice.clone() + 20, MaxLattice::new(68));
/// assert_eq!((lattice - 37).reveal(), &11);
///
/// // the `+=` and `-=` operators for in-place modification are supported too
/// let mut lattice = MaxLattice::new(2);
/// lattice += 6;
/// assert_eq!(lattice.reveal(), &8);
/// lattice -= 8;
/// assert_eq!(lattice.reveal(), &0);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct MaxLattice<T> {
    element: T,
}

impl<T: Ord> MaxLattice<T> {
    /// Constructs a new lattice from the given value.
    pub fn new(element: T) -> Self {
        Self { element }
    }
}

impl<T: Ord + Clone> Lattice for MaxLattice<T> {
    type Element = T;

    fn reveal(&self) -> &T {
        &self.element
    }

    fn into_revealed(self) -> T {
        self.element
    }

    fn merge_element(&mut self, element: &T) {
        if &self.element < element {
            self.element = element.clone();
        }
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }
}

impl<T: ops::Add> ops::Add<T> for MaxLattice<T> {
    type Output = MaxLattice<T::Output>;

    fn add(self, rhs: T) -> Self::Output {
        MaxLattice {
            element: self.element + rhs,
        }
    }
}

impl<T: ops::AddAssign> ops::AddAssign<T> for MaxLattice<T> {
    fn add_assign(&mut self, rhs: T) {
        self.element += rhs;
    }
}

impl<T: ops::Sub> ops::Sub<T> for MaxLattice<T> {
    type Output = MaxLattice<T::Output>;

    fn sub(self, rhs: T) -> Self::Output {
        MaxLattice {
            element: self.element - rhs,
        }
    }
}

impl<T: ops::SubAssign> ops::SubAssign<T> for MaxLattice<T> {
    fn sub_assign(&mut self, rhs: T) {
        self.element -= rhs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign() {
        let mut ml = MaxLattice::default();
        assert_eq!(ml.reveal(), &0);
        ml.assign(10u64);
        assert_eq!(ml.reveal(), &10);
        ml.assign(5);
        assert_eq!(ml.reveal(), &5);
    }

    #[test]
    fn merge_by_value() {
        let mut ml = MaxLattice::default();
        assert_eq!(ml.reveal(), &0);
        ml.merge_element(&10u64);
        assert_eq!(ml.reveal(), &10);
        ml.merge_element(&5);
        assert_eq!(ml.reveal(), &10);
        ml.merge_element(&11);
        assert_eq!(ml.reveal(), &11);
    }

    #[test]
    fn merge_by_lattice() {
        let mut ml = MaxLattice::default();
        assert_eq!(ml.reveal(), &0);
        ml.merge(&MaxLattice::new(10u64));
        assert_eq!(ml.reveal(), &10);
        ml.merge(&MaxLattice::new(5));
        assert_eq!(ml.reveal(), &10);
        ml.merge(&MaxLattice::new(11));
        assert_eq!(ml.reveal(), &11);
    }

    #[test]
    fn add() {
        let mut ml = MaxLattice::default();
        assert_eq!(ml.reveal(), &0);
        ml += 5;
        assert_eq!(ml.reveal(), &5);
        assert_eq!((ml + 2).reveal(), &7);
    }

    #[test]
    fn sub() {
        let mut ml = MaxLattice::default();
        assert_eq!(ml.reveal(), &0);
        ml -= 5;
        assert_eq!(ml.reveal(), &-5);
        assert_eq!((ml - 2).reveal(), &-7);
    }
}
