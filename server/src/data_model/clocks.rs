use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
    RwLock,
};

use serde::{Deserialize, Serialize};

/// A thread-safe vector clock using AtomicU64 for clock values.
#[derive(Clone, Debug)]
pub struct VectorClock {
    clock: Arc<RwLock<AtomicU64>>,
}

impl VectorClock {
    /// Create a new vector clock initialized to one.
    pub fn new() -> Self {
        Self {
            clock: Arc::new(RwLock::new(AtomicU64::new(1))),
        }
    }

    /// Create a new vector clock initialized to a specific value.
    /// This is used only for deserialization from stored values in the state
    /// store.
    fn new_with_value(value: u64) -> Self {
        Self {
            clock: Arc::new(RwLock::new(AtomicU64::new(value))),
        }
    }

    /// Increment the clock value for a given key and return the new value.
    /// It uses SeqCst ordering to ensure strong consistency across threads.
    #[allow(dead_code)]
    pub fn increment(&self) -> u64 {
        let value = self.clock.write().unwrap();

        value.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current clock value for a given key.
    /// It uses SeqCst ordering to ensure strong consistency across threads.
    pub fn value(&self) -> u64 {
        let value = self.clock.read().unwrap();
        value.load(Ordering::SeqCst)
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
        Self::new_with_value(value)
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

impl Serialize for VectorClock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.value())
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
