use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

#[async_trait]
pub trait Cache<K, V>: Send + Sync
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, key: &K) -> Result<Option<V>, anyhow::Error>;
    async fn insert(&mut self, key: K, value: V) -> Result<(), anyhow::Error>;
    async fn invalidate(&mut self, key: &K) -> Result<(), anyhow::Error>;
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
    fn serialize_to_flexbuffer(&self) -> Result<Vec<u8>, anyhow::Error> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    fn deserialize_from_flexbuffer(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        let r = flexbuffers::Reader::get_root(bytes)?;
        let value = Self::deserialize(r)?;
        Ok(value)
    }
}

impl<T> FlexBufferable for T where T: Cacheable {}

pub trait Cacheable: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {}
impl<T> Cacheable for T where T: Send + Sync + Serialize + DeserializeOwned + Clone + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

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