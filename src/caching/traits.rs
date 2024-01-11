use anyhow::Result;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

#[async_trait]
pub trait Cache<K, V>
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
    Serialize
    + DeserializeOwned
    + Send
    + Sync
    + Clone
    + PartialEq
    + Eq
    + std::hash::Hash
    + std::fmt::Debug
    + 'static
{
}
impl<T> CacheKey for T where
    T: Serialize
        + DeserializeOwned
        + Send
        + Sync
        + Clone
        + PartialEq
        + Eq
        + std::hash::Hash
        + std::fmt::Debug
        + 'static
{
}

/// CacheValue is a trait that must be implemented by all values used in a
/// cache. Note on use of 'static lifetime - this is required for the following
/// reasons:
/// - to ensure no unowned references are used in the value
/// - data can live as long as the cache
/// This requires that the value be cloneable, in order to be placed in the
/// cache safely.
pub trait CacheValue:
    Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}
impl<T> CacheValue for T where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}
