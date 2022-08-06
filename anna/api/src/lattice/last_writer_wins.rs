//! Provides the [`LastWriterWinsLattice`] and related timestamp types.

use super::Lattice;

/// A lattice where later writes overwrite earlier ones.
///
/// Keeps a [`Timestamp`] for the value. On merge, the value is overwritten
/// only if the timestamp in the other value is newer. By using this type, we can ensure
/// that writes to a key exhibit a total ordering, which results in _"read commited"_ consistency.
///
/// ## Example
///
/// ```
/// use anna_api::lattice::{
///     Lattice, LastWriterWinsLattice,
///     last_writer_wins::{Timestamp, TimestampValuePair},
/// };
///
/// // initialize a new Lww lattice with the current time
/// let mut lattice = LastWriterWinsLattice::from_pair(Timestamp::now(), 42);
/// assert_eq!(lattice.reveal().value(), &42);
///
/// // create two new values, keeping track of their creation time
/// let value_1 = TimestampValuePair::new(Timestamp::now(), 100);
/// let value_2 = TimestampValuePair::new(Timestamp::now(), 50);
/// assert!(value_1.timestamp() < value_2.timestamp());
///
/// // merging `value_2` overwrites the values since `value_2` was created later than the
/// // original value
/// lattice.merge_element(&value_2);
/// assert_eq!(lattice.reveal().value(), &50);
///
/// // merging `value_1` afterwards does not change anything since `value_1` was created
/// // before `value_2`
/// lattice.merge_element(&value_1);
/// assert_eq!(lattice.reveal().value(), &50);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct LastWriterWinsLattice<T> {
    element: TimestampValuePair<T>,
}

impl<T> LastWriterWinsLattice<T> {
    /// Creates a new lattice from the given value.
    pub fn new(element: TimestampValuePair<T>) -> Self {
        Self { element }
    }

    /// Creates a new lattice from the given value, using the current time as timestamp
    pub fn new_now(value: T) -> Self {
        Self::from_pair(Timestamp::now(), value)
    }

    /// Convenience function to construct a lattice from a timestamp and a value.
    pub fn from_pair(timestamp: Timestamp, value: T) -> Self {
        Self::new(TimestampValuePair::new(timestamp, value))
    }

    /// Get a reference to the wrapped `TimestampValuePair`.
    pub fn element(&self) -> &TimestampValuePair<T> {
        &self.element
    }
}

impl<T> Lattice for LastWriterWinsLattice<T>
where
    T: Clone + PartialEq + std::fmt::Debug,
{
    type Element = TimestampValuePair<T>;

    fn reveal(&self) -> &TimestampValuePair<T> {
        &self.element
    }

    fn into_revealed(self) -> TimestampValuePair<T> {
        self.element
    }

    fn assign(&mut self, element: Self::Element) {
        self.element = element;
    }

    fn merge_element(&mut self, element: &TimestampValuePair<T>) {
        if element.timestamp > self.element.timestamp {
            self.element = element.clone();
        } else if element.timestamp == self.element.timestamp {
            // TODO: Instead of panicking if values are different, use ar arbitrary
            // deterministic mechanism to declare a winner. For example, sort by the
            // raw byte representation of the values. The only requirement is that all
            // nodes reach the same state, i.e. that the state becomes consistent.
            assert_eq!(
                element, &self.element,
                "merge error: LwwLattices have identical timestamps but different values"
            );
        }
    }
}

/// The element type stored in a [`LastWriterWinsLattice`]. Pair of a [`Timestamp`] and a value.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimestampValuePair<T> {
    timestamp: Timestamp,
    value: T,
}

impl<T> TimestampValuePair<T> {
    /// Constructs a new pair from the given timestamp and value.
    pub fn new(timestamp: Timestamp, value: T) -> Self {
        Self { timestamp, value }
    }

    /// Returns the stored timestamp.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns a reference to the wrapped value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Returns a reference to the wrapped value, taking ownership.
    pub fn into_value(self) -> T {
        self.value
    }
}

/// The UTC timestamp used for keeping track of value creation times.
///
/// Used to determine which value is the newest for [`LastWriterWinsLattice`] instances.
///
/// Depends on the system time reported by the operating system.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Timestamp(chrono::DateTime<chrono::Utc>);

impl Timestamp {
    /// Returns an UTC timestamp corresponding to the current date and time.
    pub fn now() -> Self {
        Self(chrono::Utc::now())
    }
}
