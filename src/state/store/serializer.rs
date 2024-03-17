use serde::de::DeserializeOwned;

use super::StateMachineError;

pub struct Serializer;

pub trait Serialize {
    fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StateMachineError>;
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateMachineError>;
}

impl Serialize for Serializer {
    fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, StateMachineError> {
        serde_json::to_vec(value).map_err(|e| StateMachineError::SerializationError(e.into()))
    }

    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateMachineError> {
        serde_json::from_slice(bytes).map_err(|e| StateMachineError::SerializationError(e.into()))
    }
}
