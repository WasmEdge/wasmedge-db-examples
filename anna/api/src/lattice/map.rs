use super::{BoolLattice, Lattice, SetLattice};
use std::{borrow::Borrow, collections::HashMap, hash::Hash};

/// [`HashMap`]-based lattice that stores other lattice types as values.
///
/// The merge operation takes the union of the key set of both maps. For keys that are present
/// in both maps, the two values are merged using their merge function.
///
/// ## Example
///
/// ```
/// use anna_api::lattice::{Lattice, MapLattice, MaxLattice};
/// use std::collections::{HashMap, HashSet};
///
/// // initialize a new `HashMap` with value type `MaxLattice`
/// let mut map = HashMap::new();
/// map.insert("foo", MaxLattice::new(5));
/// map.insert("bar", MaxLattice::new(12));
/// map.insert("baz", MaxLattice::new(42));
///
/// // create a new MapLattice from the map
/// let mut lattice = MapLattice::new(map.clone());
/// assert_eq!(lattice.reveal(), &map);
///
/// // merge some other map
/// let mut other_map = HashMap::new();
/// other_map.insert("bar", MaxLattice::new(16));
/// other_map.insert("foo", MaxLattice::new(2));
/// other_map.insert("foobar", MaxLattice::new(732));
/// lattice.merge_element(&other_map);
///
/// // the result contains all the keys from both maps
/// let mut keys: HashSet<_> = lattice.reveal().keys().copied().collect();
/// assert_eq!(keys, ["bar", "foo", "foobar", "baz"].iter().copied().collect());
///
/// // the values for `baz` and `foobar` only appear in one map each, so they're not changed
/// assert_eq!(lattice.reveal().get("baz"), Some(&MaxLattice::new(42)));
/// assert_eq!(lattice.reveal().get("foobar"), Some(&MaxLattice::new(732)));
///
/// // the two other values are present in both maps, so they're merged using the merge
/// // operation of `MaxLattice` (which returns the maximum of the two numbers)
/// assert_eq!(lattice.reveal().get("foo"), Some(&MaxLattice::new(5)));
/// assert_eq!(lattice.reveal().get("bar"), Some(&MaxLattice::new(16)));
/// ```
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MapLattice<K, V> {
    #[serde(bound = "
        K: Hash + Eq + serde::Serialize + for<'a> serde::Deserialize<'a>,
        V: serde::Serialize + for<'a> serde::Deserialize<'a>,
    ")]
    element: HashMap<K, V>,
}

impl<K, V> Lattice for MapLattice<K, V>
where
    K: Eq + Hash + Clone,
    V: Lattice + Clone,
{
    type Element = HashMap<K, V>;

    fn reveal(&self) -> &HashMap<K, V> {
        &self.element
    }

    fn into_revealed(self) -> HashMap<K, V> {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, elements: &HashMap<K, V>) {
        for (key, value) in elements {
            match self.element.entry(key.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    // entry is not in the map yet -> insert it
                    entry.insert(value.clone());
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // merge new entry
                    entry.get_mut().merge(value);
                }
            };
        }
    }
}

impl<K, V> MapLattice<K, V>
where
    K: Eq + Hash,
{
    /// Creates a new lattice from the given map.
    pub fn new(element: HashMap<K, V>) -> Self {
        Self { element }
    }

    /// Inserts the given value into the map, merging it with the previous value if any.
    ///
    /// If no value with the given key is in the map yet, the given value is inserted. If a
    /// previous value exists, the new value is merged into it (as defined by the value's
    /// [`Lattice`] implementation).
    pub fn insert(&mut self, key: K, value: V)
    where
        V: Lattice,
    {
        match self.element.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                // entry is not in the map yet -> insert it
                entry.insert(value);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                // merge new entry
                entry.get_mut().merge(&value);
            }
        };
    }

    /// Removes the value associated with the given key from the map.
    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.element.remove(key);
    }

    /// Returns the set of keys present in this map as a [`SetLattice`].
    pub fn key_set(&self) -> SetLattice<K>
    where
        K: Clone,
    {
        SetLattice::new(self.element.keys().cloned().collect())
    }

    /// Returns `true` if the map contains a value for the given key.
    pub fn contains_key(&self, key: &K) -> BoolLattice {
        BoolLattice::new(self.element.contains_key(key))
    }
}

impl<K, V> Default for MapLattice<K, V> {
    fn default() -> Self {
        Self {
            element: Default::default(),
        }
    }
}

impl<K, V> PartialEq for MapLattice<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.element == other.element
    }
}

impl<K, V> Eq for MapLattice<K, V>
where
    K: Eq + Hash,
    V: PartialEq,
{
}
