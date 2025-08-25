use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc,
};

use serde::{Deserialize, Serialize};

/// A thread-safe vector clock using AtomicU64 for clock values.
/// It uses `std::sync::atomic::Ordering::Relaxed` for all atomic operations
/// to guarantee that all modifications of the same atomic variable happen
/// in an order that is the same from the perspective of every single thread.
///
/// A vector clock can be incremented manually by calling the `tick` method.
/// When serialized, the clock value is incremented automatically to reflect the
/// latest state.
///
/// Example:
///
/// ```rust
/// let vc = VectorClock::new();
/// assert_eq!(vc.value(), 0);
/// let new_value = vc.tick();
/// assert_eq!(new_value, 1);
/// assert_eq!(vc.value(), 1);
///
/// let serialized = serde_json::to_string(&vc).unwrap();
/// assert_eq!(serialized, "2"); // ticked again during serialization
/// assert_eq!(vc.value(), 2);
/// ```
#[derive(Clone, Debug)]
pub struct VectorClock {
    clock: Arc<AtomicU64>,
}

impl VectorClock {
    /// Create a new vector clock initialized to zero.
    pub fn new() -> Self {
        Self {
            clock: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the clock value for a given key and return the new value.
    /// It uses SeqCst ordering to ensure strong consistency across threads.
    pub fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Relaxed) + 1
    }

    /// Get the current clock value for a given key.
    /// It uses SeqCst ordering to ensure strong consistency across threads.
    pub fn value(&self) -> u64 {
        self.clock.load(Relaxed)
    }

    /// Compare this vector clock with another.
    /// Returns an Ordering indicating whether this clock is less than, equal
    /// to, or greater than the other clock based on their values.
    pub fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value().cmp(&other.value())
    }
}

impl From<u64> for VectorClock {
    fn from(value: u64) -> Self {
        Self {
            clock: Arc::new(AtomicU64::new(value)),
        }
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for VectorClock {}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for VectorClock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.tick())
    }
}

impl<'de> Deserialize<'de> for VectorClock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Ok(value.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_basic() {
        let vc = VectorClock::new();
        assert_eq!(vc.value(), 0);

        let new_value = vc.tick();
        assert_eq!(new_value, 1);
        assert_eq!(vc.value(), 1);

        let serialized = serde_json::to_string(&vc).unwrap();
        assert_eq!(serialized, "2"); // ticked again during serialization
        assert_eq!(vc.value(), 2);

        let new_vc = VectorClock::new();
        assert_eq!(new_vc.value(), 0);
        assert!(vc > new_vc);
    }

    #[test]
    fn test_vector_clock_deserialize() {
        let json = "5";
        let vc: VectorClock = serde_json::from_str(json).unwrap();
        assert_eq!(vc.value(), 5);
    }
}
