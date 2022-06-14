use super::{Lattice, MaxLattice};
use std::collections::BTreeSet;

/// Lattice implementation based on an ordered [`BTreeSet`] with the
/// [union operation](https://en.wikipedia.org/wiki/Union_(set_theory)) as merge operator.
///
/// ## Example
///
/// ```
/// use anna_api::lattice::{Lattice, OrderedSetLattice};
/// use std::collections::BTreeSet;
///
/// // initialize a new SetLattice with a few items
/// let set: BTreeSet<_> = [144, 540, 156, 58].iter().copied().collect();
/// let mut lattice = OrderedSetLattice::new(set.clone());
/// assert_eq!(lattice.reveal(), &set);
///
/// // the set is always ordered
/// assert_eq!(lattice.reveal().iter().copied().collect::<Vec<_>>(), vec![58, 144, 156, 540]);
///
/// // merge some other set
/// let set_2: BTreeSet<_> = [8, 204, 156].iter().copied().collect();;
/// lattice.merge_element(&set_2);
///
/// // the result is the union of the two sets
/// let union = set.union(&set_2).copied().collect::<BTreeSet<_>>();
/// assert_eq!(lattice.reveal(), &union);
///
/// // and it's still ordered
/// assert_eq!(
///     lattice.reveal().iter().copied().collect::<Vec<_>>(),
///     vec![8, 58, 144, 156, 204, 540]
/// );
/// ```
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct OrderedSetLattice<T> {
    #[serde(bound = "T: Ord + serde::Serialize + for<'a> serde::Deserialize<'a>")]
    element: BTreeSet<T>,
}

impl<T> OrderedSetLattice<T> {
    /// Creates a new lattice based on the given set.
    pub fn new(element: BTreeSet<T>) -> Self {
        Self { element }
    }

    /// Returns the number of elements in this set.
    pub fn len(&self) -> MaxLattice<usize> {
        MaxLattice::new(self.element.len())
    }

    /// Returns an iterator that yields all items that are in both `self` and `other`.
    pub fn intersection<'a>(&'a self, other: &'a BTreeSet<T>) -> impl Iterator<Item = &'a T>
    where
        T: Ord,
    {
        self.element.intersection(other)
    }
}

impl<T> Lattice for OrderedSetLattice<T>
where
    T: Eq + Ord + Clone,
{
    type Element = BTreeSet<T>;

    fn reveal(&self) -> &BTreeSet<T> {
        &self.element
    }

    fn into_revealed(self) -> BTreeSet<T> {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, element: &BTreeSet<T>) {
        for val in element {
            if !self.element.contains(val) {
                self.element.insert(val.clone());
            }
        }
    }
}

impl<T> Default for OrderedSetLattice<T>
where
    T: Ord,
{
    fn default() -> Self {
        Self {
            element: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign() {
        let set1: BTreeSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: BTreeSet<char> = ['c', 'd', 'e'].iter().copied().collect();

        let mut sl = OrderedSetLattice::default();
        assert_eq!(sl.len().reveal(), &0);
        sl.assign(set1.clone());
        assert_eq!(sl.len().reveal(), &3);
        assert_eq!(sl.reveal(), &set1);

        sl.assign(set2.clone());
        assert_eq!(sl.len().reveal(), &3);
        assert_eq!(sl.reveal(), &set2);
    }

    #[test]
    fn merge_by_value() {
        let set1: BTreeSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: BTreeSet<char> = ['c', 'd', 'e'].iter().copied().collect();
        let set3: BTreeSet<char> = ['a', 'b', 'c', 'd', 'e'].iter().copied().collect();

        let mut sl = OrderedSetLattice::default();
        assert_eq!(sl.len().reveal(), &0);

        sl.merge_element(&set1);
        assert_eq!(sl.len().reveal(), &3);
        assert_eq!(sl.reveal(), &set1);

        sl.merge_element(&set2);
        assert_eq!(sl.len().reveal(), &5);
        assert_eq!(sl.reveal(), &set3);
    }

    #[test]
    fn merge_by_lattice() {
        let set1: BTreeSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: BTreeSet<char> = ['c', 'd', 'e'].iter().copied().collect();
        let set3: BTreeSet<char> = ['a', 'b', 'c', 'd', 'e'].iter().copied().collect();

        let mut sl = OrderedSetLattice::default();

        sl.merge(&OrderedSetLattice::new(set1.clone()));
        assert_eq!(sl.len().reveal(), &3);
        assert_eq!(sl.reveal(), &set1);

        sl.merge(&OrderedSetLattice::new(set2));
        assert_eq!(sl.len().reveal(), &5);
        assert_eq!(sl.reveal(), &set3);
    }

    #[test]
    fn intersection() {
        let set1: BTreeSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: BTreeSet<char> = ['c', 'd', 'e'].iter().copied().collect();

        let mut sl = OrderedSetLattice::default();
        sl.merge(&OrderedSetLattice::new(set1.clone()));
        assert_eq!(sl.len().reveal(), &3);
        assert_eq!(sl.reveal(), &set1);

        let res = sl.intersection(&set2);
        assert_eq!(res.copied().collect::<Vec<_>>(), vec!['c']);
    }

    #[test]
    fn ordering() {
        let set1: BTreeSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: BTreeSet<char> = ['c', 'd', 'e'].iter().copied().collect();

        let mut sl = OrderedSetLattice::default();
        sl.merge_element(&set2);
        assert_eq!(sl.reveal().iter().next(), Some(&'c'));

        sl.merge_element(&set1);
        assert_eq!(sl.reveal().iter().next(), Some(&'a'));
    }
}
