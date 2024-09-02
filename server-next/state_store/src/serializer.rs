use std::{any::type_name, fmt::Debug};

use anyhow::Result;
use serde::de::DeserializeOwned;

pub struct JsonEncoder;

pub trait JsonEncode {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T>;
}

impl JsonEncode for JsonEncoder {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| {
            anyhow::anyhow!(
                "error serializing into json: {}, type: {}, value: {:?}",
                e,
                type_name::<T>(),
                value
            )
        })
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| {
            anyhow::anyhow!(
                "error deserializing from json bytes, {}, value: {:?}",
                e,
                type_name::<T>()
            )
        })
    }
}
