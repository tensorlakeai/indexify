pub use super::prelude::*;

pub struct RedisCache<K, V>
{
    client: redis::Client,
    _k: std::marker::PhantomData<K>,
    _v: std::marker::PhantomData<V>,
}

impl <K, V> RedisCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    pub fn new(client: redis::Client) -> Self {
        Self {
            client,
            _k: std::marker::PhantomData,
            _v: std::marker::PhantomData,
        }
    }
}

impl From<redis::Client> for RedisCache<String, String> {
	fn from(client: redis::Client) -> Self {
		Self::new(client)
	}
}

#[async_trait]
impl <K, V> Cache<K, V> for RedisCache<K, V>
where
    K: CacheKey,
    V: CacheValue,
{
    async fn get(&self, key: &K) -> Result<Option<V>> {
        let mut conn = self.client.get_async_connection().await?;
        let key = serde_json::to_string(key)?;
        let value: Option<String> = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
        if let Some(value) = value {
            let value = serde_json::from_str(&value)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
    async fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        let key = serde_json::to_string(&key)?;
        let value = serde_json::to_string(&value)?;
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }
    async fn invalidate(&mut self, key: &K) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        let key = serde_json::to_string(key)?;
        redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }
}

// TODO: integrate Redis into local dev setup so we can run this test
// #[cfg(test)]
// mod tests {
//     use super::*;
// 	use crate::caching::traits::Cache;
//     use tokio;

//     #[tokio::test]
//     async fn test_redis_cache_with_extractor_types() {
		
// 		let key = serde_json::Value::String("test".to_string()).to_string();
		
// 		// use Vec<u8, Global>
//         let value: Vec<u8> = vec![1, 2, 3, 4, 5];
// 		let mut cache: RedisCache<String, Vec<u8>>;
// 		match redis::Client::open("redis://localhost:6379") {
// 			Ok(client) => {
// 				cache = RedisCache::<String, Vec<u8>>::new(client);
// 			},
// 			Err(e) => {
// 				panic!("Unable to open redis client. Is redis running? Error: {}", e);
// 			}
// 		}

//         // Test insert
//         cache.insert(key.clone(), value.clone()).await.unwrap();

//         // Test get
//         let cached_value = cache.get(&key).await.unwrap();
//         assert_eq!(cached_value, Some(value));

//         // Test invalidate
//         cache.invalidate(&key).await.unwrap();
//         let cached_value = cache.get(&key).await.unwrap();
//         assert!(cached_value.is_none());
//     }
// }
