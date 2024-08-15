use std::sync::Arc;

use anyhow::Result;
use indexify_internal_api::{Content, ExtractResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

use super::prelude::*;
use crate::{
    caching::{MokaAsyncCache, NoOpCache, RedisCache},
    server_config::{ServerCacheBackend, ServerCacheConfig},
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtractContentCacheKey {
    content: Arc<Content>,
    input_params: Arc<Option<serde_json::Value>>,
}
pub type CacheTS<K, V> = Arc<RwLock<Box<dyn Cache<K, V>>>>;

#[allow(dead_code)]
#[derive(Clone)]
pub struct Caches {
    pub cache_extract_content: Arc<RwLock<Box<dyn Cache<ExtractContentCacheKey, ExtractResponse>>>>,
}

impl Caches {
    pub fn new(cache_config: ServerCacheConfig) -> Self {
        Self {
            cache_extract_content: Self::create_cache(cache_config).unwrap(),
        }
    }

    fn create_cache<K, V>(cache_config: ServerCacheConfig) -> Result<CacheTS<K, V>>
    where
        K: CacheKey,
        V: CacheValue,
    {
        match cache_config.backend {
            ServerCacheBackend::None => {
                info!("no cache backend configured. Using NoOpCache");
                Ok(Arc::new(RwLock::new(Box::new(NoOpCache::new()))))
            }
            ServerCacheBackend::Memory => {
                info!("memory cache config provided. Using MoKaAsyncCache");
                if let Some(memory_config) = cache_config.memory {
                    let cache = MokaAsyncCache::new(memory_config.max_size as u64);
                    Ok(Arc::new(RwLock::new(Box::new(cache))))
                } else {
                    tracing::warn!("memory config not provided. Using default config");
                    let cache = MokaAsyncCache::default();
                    Ok(Arc::new(RwLock::new(Box::new(cache))))
                }
            }
            ServerCacheBackend::Redis => {
                info!("redis cache config provided. Using RedisCache");
                if let Some(redis_config) = cache_config.redis {
                    let client = redis::Client::open(redis_config.addr)?;
                    let cache = RedisCache::new(client);
                    Ok(Arc::new(RwLock::new(Box::new(cache))))
                } else {
                    // TODO: Is a panic the right thing to do here? We could log an error, but it
                    // would be easy to miss
                    panic!("redis chosen as cache backend, but no redis config provided. Check your server config")
                }
            }
        }
    }
}
