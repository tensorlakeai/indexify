use std::{any::type_name, fmt::Debug, sync::LazyLock};

use anyhow::Result;
use opentelemetry::metrics::Histogram;
use serde::de::DeserializeOwned;
use tracing::warn;

/// Version byte prefix for binary-encoded values.
const BINARY_VERSION: u8 = 0x01;

/// Threshold above which a serialized value triggers a warning log.
const LARGE_VALUE_THRESHOLD: usize = 100_000;

static VALUE_SIZE_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    let meter = opentelemetry::global::meter("state_store");
    meter
        .u64_histogram("indexify.state_store.value_size_bytes")
        .with_description("Size of serialized values written to the state store")
        .build()
});

pub struct StateStoreEncoder;

pub trait StateStoreEncode {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;
}

impl StateStoreEncode for StateStoreEncoder {
    /// Encodes a value as postcard with a version byte prefix.
    ///
    /// Format: [0x01] [postcard payload]
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>> {
        let payload = postcard::to_allocvec(value).map_err(|e| {
            anyhow::anyhow!(
                "error serializing to postcard: {}, type: {}, value: {:?}",
                e,
                type_name::<T>(),
                value
            )
        })?;
        let total_size = 1 + payload.len();
        let mut buf = Vec::with_capacity(total_size);
        buf.push(BINARY_VERSION);
        buf.extend_from_slice(&payload);
        VALUE_SIZE_HISTOGRAM.record(total_size as u64, &[]);
        if total_size > LARGE_VALUE_THRESHOLD {
            warn!(
                size_bytes = total_size,
                type_name = type_name::<T>(),
                "large value written to state store"
            );
        }
        Ok(buf)
    }

    /// Decodes a postcard value with a `0x01` version byte prefix.
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        if bytes.is_empty() {
            return Err(anyhow::anyhow!(
                "empty bytes when decoding type: {}",
                type_name::<T>()
            ));
        }

        if bytes[0] != BINARY_VERSION {
            return Err(anyhow::anyhow!(
                "unexpected version byte {:#04x} when decoding type: {} (expected binary prefix {:#04x})",
                bytes[0],
                type_name::<T>(),
                BINARY_VERSION
            ));
        }

        postcard::from_bytes(&bytes[1..]).map_err(|e| {
            anyhow::anyhow!(
                "error deserializing from postcard: {}, type: {}",
                e,
                type_name::<T>()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestStruct {
        name: String,
        value: u64,
        data: Vec<u8>,
    }

    #[test]
    fn test_postcard_round_trip() {
        let original = TestStruct {
            name: "test".to_string(),
            value: 42,
            data: vec![1, 2, 3],
        };

        let encoded = StateStoreEncoder::encode(&original).unwrap();
        assert_eq!(encoded[0], BINARY_VERSION);

        let decoded: TestStruct = StateStoreEncoder::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_non_binary_prefix_returns_error() {
        // Data without the 0x01 binary prefix should now return an error
        let original = TestStruct {
            name: "legacy".to_string(),
            value: 99,
            data: vec![4, 5, 6],
        };
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let result = StateStoreEncoder::decode::<TestStruct>(&json_bytes);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected version byte"),
        );
    }

    #[test]
    fn test_postcard_smaller_than_json() {
        let value = TestStruct {
            name: "comparison".to_string(),
            value: 12345,
            data: vec![0; 100],
        };

        let postcard_bytes = StateStoreEncoder::encode(&value).unwrap();
        let json_bytes = serde_json::to_vec(&value).unwrap();

        assert!(
            postcard_bytes.len() < json_bytes.len(),
            "postcard ({} bytes) should be smaller than json ({} bytes)",
            postcard_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn test_empty_bytes_error() {
        let result = StateStoreEncoder::decode::<TestStruct>(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_postcard_with_json_value_wrapper() {
        use crate::data_model::JsonValue;

        // Verify postcard handles JsonValue (wraps serde_json::Value as string
        // in binary formats to avoid deserialize_any).
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct WithJsonValue {
            data: JsonValue,
        }

        let original = WithJsonValue {
            data: serde_json::json!({"type": "string", "nullable": true}).into(),
        };

        let encoded = StateStoreEncoder::encode(&original).unwrap();
        let decoded: WithJsonValue = StateStoreEncoder::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }
}
