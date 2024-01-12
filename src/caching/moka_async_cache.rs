use std::marker::PhantomData;

pub use super::prelude::*;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub struct MokaAsyncCache<K, V> {
    cache: moka::future::Cache<Key, Value>,

    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

#[async_trait]
impl<K, V> Cache<K, V> for MokaAsyncCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, key: &K) -> Result<Option<V>, IndexifyCachingError> {
        let k = key.serialize_to_flexbuffer()?;
        let result = self.cache.get(&k).await;
        let result = match result {
            Some(v) => v,
            None => return Ok(None),
        };
        let r = flexbuffers::Reader::get_root(result.as_slice())?;
        let value = V::deserialize(r)?;
        Ok(Some(value))
    }

    async fn insert(&mut self, key: K, value: V) -> Result<(), IndexifyCachingError> {
        let k = key.clone().serialize_to_flexbuffer()?;
        let v = value.clone().serialize_to_flexbuffer()?;

        self.cache.insert(k, v).await;
        Ok(())
    }

    async fn invalidate(&mut self, key: &K) -> Result<(), IndexifyCachingError> {
        let k: Vec<u8> = key.serialize_to_flexbuffer()?;

        self.cache.invalidate(&k).await;
        Ok(())
    }
}

impl<K, V> MokaAsyncCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    pub fn new(max_capacity: u64) -> Self {
        let cache = moka::future::Cache::new(max_capacity);
        Self {
            cache,
            _phantom_k: PhantomData,
            _phantom_v: PhantomData,
        }
    }

    pub fn new_from_builder(
        builder: moka::future::CacheBuilder<Key, Value, moka::future::Cache<Key, Value>>,
    ) -> Self {
        let cache = builder.build();
        Self {
            cache,
            _phantom_k: PhantomData,
            _phantom_v: PhantomData,
        }
    }
}

impl<K, V> Default for MokaAsyncCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    fn default() -> Self {
        let builder = moka::future::CacheBuilder::default();
        Self::new_from_builder(builder)
    }
}
#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tokio;

    use super::*;
    use crate::caching::traits::Cache;

    #[derive(Serialize, Deserialize, Clone)]
    struct TestKey {
        key: String,
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct TestValue {
        value: Vec<u8>,
    }

    #[tokio::test]
    async fn test_moka_async_cache() {
        let mut cache = MokaAsyncCache::new(100);

        let key = TestKey {
            key: "test_key".to_string(),
        };

        let value: TestValue = TestValue {
            value: vec![1, 2, 3, 4, 5],
        };

        // Test insert
        cache.insert(key.clone(), value.clone()).await.unwrap();

        // Test get
        let cached_value = cache.get(&key).await.unwrap();
        assert_eq!(cached_value, Some(value));

        // Test invalidate
        cache.invalidate(&key).await.unwrap();
        let cached_value = cache.get(&key).await.unwrap();
        assert!(cached_value.is_none());
    }
}
