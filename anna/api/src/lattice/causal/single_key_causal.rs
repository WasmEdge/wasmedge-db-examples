use crate::lattice::{causal::VectorClock, Lattice};

/// Ensures [causal consistency](https://en.wikipedia.org/wiki/Causal_consistency) for a lattice
/// value.
///
/// By using a vector clock, we can detect whether one value causally follows the other on
/// merging. If so, we update the lattice to the causally newer value. Otherwise, the values
/// were written concurrently, which is indicated by the incompatible vector clocks. In that
/// case, we merge the two values using their [`Lattice`] implementation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SingleKeyCausalLattice<T> {
    element: VectorClockValuePair<T>,
}

impl<T> SingleKeyCausalLattice<T> {
    /// Creates a new lattice from the given vector clock and value.
    pub fn new(element: VectorClockValuePair<T>) -> Self {
        Self { element }
    }
}

impl<T> Lattice for SingleKeyCausalLattice<T>
where
    T: Lattice + Clone,
{
    type Element = VectorClockValuePair<T>;

    fn reveal(&self) -> &VectorClockValuePair<T> {
        &self.element
    }

    fn into_revealed(self) -> VectorClockValuePair<T> {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, other: &VectorClockValuePair<T>) {
        let prev = self.element.vector_clock.clone();
        self.element.vector_clock.merge(&other.vector_clock);

        if self.element.vector_clock == other.vector_clock {
            // the value in `other` is strictly newer, i.e. in causally follows `self`
            self.element.value = other.value.clone();
        } else if self.element.vector_clock == prev {
            // our value is strictly newer, i.e. `self` causally follows `other -> nothing to do
        } else {
            // the value was written concurrently -> merge the values using their `Lattice` impl
            self.element.value.merge(&other.value);
        }
    }
}

/// Pair of a [`VectorClock`] and a value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[allow(missing_docs)]
pub struct VectorClockValuePair<T> {
    pub vector_clock: VectorClock,
    pub value: T,
}

impl<T> VectorClockValuePair<T> {
    /// Constructs a new pair of the given values.
    pub fn new(vector_clock: VectorClock, value: T) -> Self {
        Self {
            vector_clock,
            value,
        }
    }
}
