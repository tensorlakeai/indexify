use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc,
    },
};

use serde::{
    de::{self, Visitor},
    Deserialize,
    Serialize,
};

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

    /// Increment the clock value and return the new value.
    /// It uses Relaxed ordering to guarantee in happens-before ordering
    /// accross threads.
    pub fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Relaxed) + 1
    }

    /// Get the current clock value for a given key.
    /// It uses Relaxed ordering to guarantee in happens-before ordering
    /// accross threads.
    pub fn value(&self) -> u64 {
        self.clock.load(Relaxed)
    }

    /// Compare this vector clock with another.
    /// Returns an `std::cmp::Ordering` indicating whether this clock is less
    /// than, equal to, or greater than the other clock based on their
    /// values.
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

/// Custom deserializer to handle missing or null fields by initializing
/// the vector clock to zero.
///
/// This ensures that data already existing in the state store without
/// a vector clock field can be deserialized correctly.
struct VectorClockVisitor;

impl<'de> Visitor<'de> for VectorClockVisitor {
    type Value = VectorClock;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an unsigned integer representing the vector clock or null/missing")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value.into())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(VectorClock::new())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_u64(self)
    }
}

impl<'de> Deserialize<'de> for VectorClock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_option(VectorClockVisitor)
    }
}

/// Linearizable is a trait that describes objects
/// that can be compared using vector clocks.
pub trait Linearizable {
    fn vector_clock(&self) -> VectorClock;
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::data_model::{Allocation, AllocationBuilder, AllocationTarget, TaskOutcome};

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

    #[test]
    fn test_vector_clock_deserialize_into_struct() {
        #[derive(Deserialize)]
        struct TestStruct {
            task_id: u32,
            clock: VectorClock,
        }

        let json_missing = r#"{"task_id": 1}"#;
        let ts: TestStruct = serde_json::from_str(json_missing).unwrap();
        assert_eq!(ts.task_id, 1);
        assert_eq!(ts.clock.value(), 0);

        let json_null = r#"{"task_id": 1, "clock": null}"#;
        let ts_null: TestStruct = serde_json::from_str(json_null).unwrap();
        assert_eq!(ts_null.task_id, 1);
        assert_eq!(ts_null.clock.value(), 0);

        let json_with_value = r#"{"task_id": 1, "clock": 10}"#;
        let ts_with_value: TestStruct = serde_json::from_str(json_with_value).unwrap();
        assert_eq!(ts_with_value.task_id, 1);
        assert_eq!(ts_with_value.clock.value(), 10);

        let json_invalid = r#"{"task_id": 1, "clock": "invalid"}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(json_invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_data_model_load_regression() {
        let target = AllocationTarget {
            executor_id: "executor-1".into(),
            function_executor_id: "fe-1".into(),
        };
        let allocation = AllocationBuilder::default()
            .namespace("test-ns".to_string())
            .compute_graph("graph".to_string())
            .compute_fn("fn".to_string())
            .invocation_id("invoc-1".to_string())
            .function_call_id("task-1".into())
            .input_args(vec![])
            .call_metadata(Bytes::new())
            .target(target.clone())
            .attempt_number(1)
            .outcome(TaskOutcome::Success)
            .build()
            .expect("Allocation should build successfully");

        let serialized = serde_json::to_string(&allocation).unwrap();
        let old_format = serialized.replace(",\"vector_clock\":1", "");
        assert!(!old_format.contains("vector_clock"));

        let deserialized: Allocation = serde_json::from_str(&old_format)
            .expect("failed to deserialize allocation without vector clock");
        assert_eq!(deserialized.vector_clock.value(), 0);
    }
}
