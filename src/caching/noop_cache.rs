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
    async fn get(&self, _key: &K) -> Result<Option<V>> {
        Ok(None)
    }

    async fn insert(&mut self, _key: K, _value: V) -> Result<()> {
        Ok(())
    }

    async fn invalidate(&mut self, _key: &K) -> Result<()> {
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
