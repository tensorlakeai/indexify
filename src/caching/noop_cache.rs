pub use super::prelude::*;

pub struct NoOpCache<K, V> {
    _k: std::marker::PhantomData<K>,
    _v: std::marker::PhantomData<V>,
}

#[async_trait]
impl<K, V> Cache<K, V> for NoOpCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, _key: &K) -> Result<Option<V>, IndexifyCachingError> {
        Ok(None)
    }

    async fn insert(&mut self, _key: K, _value: V) -> Result<(), IndexifyCachingError> {
        Ok(())
    }

    async fn invalidate(&mut self, _key: &K) -> Result<(), IndexifyCachingError> {
        Ok(())
    }
}

impl<K, V> NoOpCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    pub fn new() -> Self {
        Self {
            _k: std::marker::PhantomData,
            _v: std::marker::PhantomData,
        }
    }
}
