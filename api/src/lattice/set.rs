use super::Lattice;
use std::{collections::HashSet, hash::Hash};

/// Lattice implementation based on a [`HashSet`] with the
/// [union operation](https://en.wikipedia.org/wiki/Union_(set_theory)) as merge operator.
///
/// ## Example
///
/// ```
/// use anna_api::lattice::{Lattice, SetLattice};
/// use std::collections::HashSet;
///
/// // initialize a new SetLattice with a few items
/// let set: HashSet<_> = ["foo", "bar", "baz"].iter().copied().collect();
/// let mut lattice = SetLattice::new(set.clone());
/// assert_eq!(lattice.reveal(), &set);
///
/// // merge some other set
/// let set_2: HashSet<_> = ["bar", "foobar"].iter().copied().collect();
/// lattice.merge_element(&set_2);
///
/// // the result is the union of the two sets
/// let union = set.union(&set_2).copied().collect::<HashSet<_>>();
/// assert_eq!(lattice.reveal(), &union);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SetLattice<T> {
    #[serde(bound = "T: Hash + Eq + serde::Serialize + for<'a> serde::Deserialize<'a>")]
    element: HashSet<T>,
}

impl<T> SetLattice<T>
where
    T: Eq + Hash,
{
    /// Creates a new lattice based on the given set.
    pub fn new(element: HashSet<T>) -> Self {
        Self { element }
    }

    /// Inserts the given value into the set.
    pub fn insert(&mut self, element: T) {
        self.element.insert(element);
    }

    /// Creates an iterator that returns all the items that are both in `self` and `other`.
    pub fn intersection<'a>(&'a self, other: &'a HashSet<T>) -> impl Iterator<Item = &'a T> {
        self.element.intersection(other)
    }
}

impl<T> Lattice for SetLattice<T>
where
    T: Eq + Hash + Clone,
{
    type Element = HashSet<T>;

    fn reveal(&self) -> &HashSet<T> {
        &self.element
    }

    fn into_revealed(self) -> HashSet<T> {
        self.element
    }

    fn merge_element(&mut self, element: &HashSet<T>) {
        for val in element {
            if !self.element.contains(val) {
                self.element.insert(val.clone());
            }
        }
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }
}

impl<T> Default for SetLattice<T> {
    fn default() -> Self {
        Self {
            element: Default::default(),
        }
    }
}

impl<T> PartialEq for SetLattice<T>
where
    HashSet<T>: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.element == other.element
    }
}

impl<T> Eq for SetLattice<T> where HashSet<T>: Eq {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersection() {
        let set1: HashSet<char> = ['a', 'b', 'c'].iter().copied().collect();
        let set2: HashSet<char> = ['c', 'd', 'e'].iter().copied().collect();

        let mut sl = SetLattice::default();
        sl.merge_element(&set1);
        let res = sl.intersection(&set2);
        assert_eq!(res.copied().collect::<Vec<char>>(), vec!['c']);
    }
}
