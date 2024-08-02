use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use super::prelude::IndexifyCachingError;

#[allow(dead_code)]
#[async_trait]
pub trait Cache<K, V>: Send + Sync
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, key: &K) -> Result<Option<V>, IndexifyCachingError>;
    async fn insert(&mut self, key: K, value: V) -> Result<(), IndexifyCachingError>;
    async fn invalidate(&mut self, key: &K) -> Result<(), IndexifyCachingError>;
}

/// CacheKey is a trait that must be implemented by all keys used in a cache.
/// Note on use of 'static lifetime - this is required for the following
/// reasons:
/// - to ensure no unowned references are used in the key
/// - data can live as long as the cache
/// This requires that the key be cloneable, in order to be placed in the cache
/// safely.
pub trait CacheKey:
    Send + Sync + Serialize + DeserializeOwned + Clone + Cacheable + FlexBufferable + 'static
{
}

impl<T> CacheKey for T where T: Cacheable {}

/// CacheValue is a trait that must be implemented by all values used in a
/// cache. Note on use of 'static lifetime - this is required for the following
/// reasons:
/// - to ensure no unowned references are used in the value
/// - data can live as long as the cache
/// This requires that the value be cloneable, in order to be placed in the
/// cache safely.
pub trait CacheValue:
    Send + Sync + Serialize + DeserializeOwned + Clone + Cacheable + FlexBufferable + 'static
{
}

impl<T> CacheValue for T where T: Cacheable {}

pub trait FlexBufferable: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {
    fn serialize_to_flexbuffer(&self) -> Result<Vec<u8>, IndexifyCachingError> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s).map_err(|e| {
            IndexifyCachingError::Serialization(format!(
                "Could not map object to FlexBuffer serializer: {}",
                e
            ))
        })?;
        Ok(s.take_buffer())
    }

    fn deserialize_from_flexbuffer(bytes: &[u8]) -> Result<Self, IndexifyCachingError> {
        let r = flexbuffers::Reader::get_root(bytes).map_err(|e| {
            IndexifyCachingError::Deserialization(format!(
                "Couldn't read a FlexBuffer bytestream from the cache: {}",
                e
            ))
        })?;
        let value = Self::deserialize(r).map_err(|e| {
            IndexifyCachingError::Deserialization(format!(
                "Deserializer would not deserialize using FlexBuffer: {}",
                e
            ))
        })?;
        Ok(value)
    }
}

impl<T> FlexBufferable for T where T: Cacheable {}

pub trait Cacheable: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {}
impl<T> Cacheable for T where T: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    // Test FlexBufferable, which will auto-implement onto TestFlexBufferable
    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct TestFlexBufferable {
        value: String,
    }

    #[test]
    fn test_flexbufferable() {
        let test = TestFlexBufferable {
            value: "test".to_string(),
        };
        let serialized = test.serialize_to_flexbuffer().unwrap();
        let deserialized = TestFlexBufferable::deserialize_from_flexbuffer(&serialized).unwrap();
        assert_eq!(test, deserialized);
    }
}
