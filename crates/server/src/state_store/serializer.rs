use std::{any::type_name, fmt::Debug};

use anyhow::Result;
use serde::de::DeserializeOwned;

/// Version byte prefix for bincode-encoded values.
const BINCODE_VERSION: u8 = 0x01;

pub struct JsonEncoder;

pub trait JsonEncode {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;
}

impl JsonEncode for JsonEncoder {
    /// Encodes a value as bincode with a version byte prefix.
    ///
    /// Format: [0x01] [bincode payload]
    ///
    /// The version byte enables the decoder to distinguish between
    /// bincode-encoded (new) and JSON-encoded (legacy) values.
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>> {
        let encoded = bincode::serialize(value).map_err(|e| {
            anyhow::anyhow!(
                "error serializing to bincode: {}, type: {}, value: {:?}",
                e,
                type_name::<T>(),
                value
            )
        })?;
        let mut buf = Vec::with_capacity(1 + encoded.len());
        buf.push(BINCODE_VERSION);
        buf.extend(encoded);
        Ok(buf)
    }

    /// Decodes a value, supporting both bincode (version-prefixed) and
    /// legacy JSON formats.
    ///
    /// If the first byte is `0x01`, the remaining bytes are decoded as bincode.
    /// Otherwise, the entire byte slice is decoded as JSON (backward compat
    /// with data written before the bincode migration).
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        if bytes.is_empty() {
            return Err(anyhow::anyhow!(
                "empty bytes when decoding type: {}",
                type_name::<T>()
            ));
        }

        if bytes[0] == BINCODE_VERSION {
            bincode::deserialize(&bytes[1..]).map_err(|e| {
                anyhow::anyhow!(
                    "error deserializing from bincode: {}, type: {}",
                    e,
                    type_name::<T>()
                )
            })
        } else {
            // Legacy JSON-encoded data (no version prefix)
            serde_json::from_slice(bytes).map_err(|e| {
                anyhow::anyhow!(
                    "error deserializing from json bytes: {}, type: {}",
                    e,
                    type_name::<T>()
                )
            })
        }
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
    fn test_bincode_round_trip() {
        let original = TestStruct {
            name: "test".to_string(),
            value: 42,
            data: vec![1, 2, 3],
        };

        let encoded = JsonEncoder::encode(&original).unwrap();
        assert_eq!(encoded[0], BINCODE_VERSION);

        let decoded: TestStruct = JsonEncoder::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_backward_compat() {
        let original = TestStruct {
            name: "legacy".to_string(),
            value: 99,
            data: vec![4, 5, 6],
        };

        // Simulate legacy JSON-encoded data (no version prefix)
        let json_bytes = serde_json::to_vec(&original).unwrap();
        let decoded: TestStruct = JsonEncoder::decode(&json_bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bincode_smaller_than_json() {
        let value = TestStruct {
            name: "comparison".to_string(),
            value: 12345,
            data: vec![0; 100],
        };

        let bincode_bytes = JsonEncoder::encode(&value).unwrap();
        let json_bytes = serde_json::to_vec(&value).unwrap();

        assert!(
            bincode_bytes.len() < json_bytes.len(),
            "bincode ({} bytes) should be smaller than json ({} bytes)",
            bincode_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn test_empty_bytes_error() {
        let result = JsonEncoder::decode::<TestStruct>(&[]);
        assert!(result.is_err());
    }
}
