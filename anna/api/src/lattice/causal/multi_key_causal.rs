use crate::{
    lattice::{causal::VectorClock, Lattice, MapLattice},
    ClientKey,
};

/// Ensures [causal consistency](https://en.wikipedia.org/wiki/Causal_consistency) for a lattice
/// value and keeps a set dependencies.
///
/// By using a vector clock, we can detect whether one value causally follows the other on
/// merging. If so, we update the lattice to the causally newer value. Otherwise, the values
/// were written concurrently, which is indicated by the incompatible vector clocks. In that
/// case, we merge the two values using their [`Lattice`] implementation.
///
/// In addition to the value, we keep track of the vector clocks of dependencies. Like the
/// value, the dependency map is overwritten when receiving a causally following vector clock,
/// keep it unchanged when receiving an causally earlier vector clock, and apply the lattice
/// merge operator on concurrent modifications.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct MultiKeyCausalLattice<T> {
    element: MultiKeyCausalPayload<T>,
}

impl<T> MultiKeyCausalLattice<T> {
    /// Constructs a new lattice with the given payload.
    pub fn new(element: MultiKeyCausalPayload<T>) -> Self {
        Self { element }
    }
}

impl<T> Lattice for MultiKeyCausalLattice<T>
where
    T: Lattice + Clone,
{
    type Element = MultiKeyCausalPayload<T>;

    fn reveal(&self) -> &MultiKeyCausalPayload<T> {
        &self.element
    }

    fn into_revealed(self) -> MultiKeyCausalPayload<T> {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, other: &MultiKeyCausalPayload<T>) {
        let prev = self.element.vector_clock.clone();
        self.element.vector_clock.merge(&other.vector_clock);

        if self.element.vector_clock == other.vector_clock {
            // incoming version is dominating
            self.element
                .dependencies
                .assign(other.dependencies.reveal().clone());
            self.element.value = other.value.clone();
        } else if self.element.vector_clock == prev {
            // our version is dominating -> nothing to do
        } else {
            // versions are concurrent
            self.element.dependencies.merge(&other.dependencies);
            self.element.value.merge(&other.value);
        }
    }
}

/// A value that can be stored in a [`MultiKeyCausalLattice`].
///
/// Contains a [`VectorClock`] for keeping track of the causal order of modifications. In
/// addition, it stores a [`MapLattice`] of the vector clocks of dependencies of the value.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(missing_docs)]
pub struct MultiKeyCausalPayload<T> {
    pub vector_clock: VectorClock,
    pub dependencies: MapLattice<ClientKey, VectorClock>,
    pub value: T,
}

impl<T> MultiKeyCausalPayload<T> {
    /// Constructs a new payload from the given values.
    pub fn new(
        vector_clock: VectorClock,
        dependencies: MapLattice<ClientKey, VectorClock>,
        value: T,
    ) -> Self {
        Self {
            vector_clock,
            dependencies,
            value,
        }
    }
}
