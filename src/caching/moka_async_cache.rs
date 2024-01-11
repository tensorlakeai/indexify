pub use super::prelude::*;

pub struct MokaAsyncCache<K, V> {
    cache: moka::future::Cache<K, V>,
}

#[async_trait]
impl<K, V> Cache<K, V> for MokaAsyncCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(self.cache.get(key).await)
    }

    async fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.cache.insert(key, value).await;
        Ok(())
    }

    async fn invalidate(&mut self, key: &K) -> Result<()> {
        self.cache.invalidate(key).await;
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
        Self { cache }
    }

    pub fn new_from_builder(
        builder: moka::future::CacheBuilder<K, V, moka::future::Cache<K, V>>,
    ) -> Self {
        let cache = builder.build();
        Self { cache }
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
    use tokio;

    use super::*;
    use crate::caching::traits::Cache;

    #[tokio::test]
    async fn test_moka_async_cache_with_extractor_types() {
        let mut cache = MokaAsyncCache::<String, Vec<u8>>::new(100);

        let key = serde_json::Value::String("test".to_string()).to_string();

        // use Vec<u8, Global>
        let value: Vec<u8> = vec![1, 2, 3, 4, 5];

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
