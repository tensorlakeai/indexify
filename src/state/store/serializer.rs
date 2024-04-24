use std::{any::type_name, fmt::Debug};

use serde::de::DeserializeOwned;

use super::StateMachineError;

pub struct JsonEncoder;

pub trait JsonEncode {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>, StateMachineError>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateMachineError>;
}

impl JsonEncode for JsonEncoder {
    fn encode<T: serde::Serialize + Debug>(value: &T) -> Result<Vec<u8>, StateMachineError> {
        serde_json::to_vec(value).map_err(|e| StateMachineError::SerializationError(format!("error serializing into json: {}, type: {}, value: {:?}", e.to_string(), type_name::<T>(), value)))
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateMachineError> {
        serde_json::from_slice(bytes).map_err(|e| StateMachineError::SerializationError(format!("error deserializing from json bytes, {}, value: {:?}", e.to_string(), type_name::<T>())))
    }
}
