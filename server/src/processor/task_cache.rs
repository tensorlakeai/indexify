use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use tokio::sync::Mutex;
use tracing::{debug, span};

use crate::{
    data_model::{CacheKey, DataPayload},
    state_store::IndexifyState,
};

pub struct CacheState {
    indexify_state: Arc<IndexifyState>,
    outputs_ingested: usize,
    cache_checks: usize,
    cache_hits: usize,
    map: HashMap<String, HashMap<(CacheKey, String), DataPayload>>,
}

pub struct TaskCache {
    mutex: Mutex<CacheState>,
}

impl TaskCache {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self {
            mutex: Mutex::new(CacheState {
                indexify_state,
                outputs_ingested: 0,
                cache_checks: 0,
                cache_hits: 0,
                map: HashMap::new(),
            }),
        }
    }

    pub async fn cache_inputs(
        &self,
        namespace: &str,
        cache_key: &CacheKey,
        inputs: &[DataPayload],
        output: &DataPayload,
    ) {
        let _span = span!(tracing::Level::DEBUG, "cache_write").entered();
        let mut hasher = DefaultHasher::new();
        for input in inputs {
            input.sha256_hash.hash(&mut hasher);
        }
        let input_hash = format!("{:x}", hasher.finish());

        debug!("Caching the output of {namespace}/{cache_key:?}/{input_hash}");

        self.mutex
            .lock()
            .await
            .map
            .entry(namespace.to_string())
            .or_default()
            .insert((cache_key.clone(), input_hash.clone()), output.clone());
    }

    pub async fn cached_outputs(
        &self,
        namespace: &str,
        cache_key: &CacheKey,
        inputs: &[DataPayload],
    ) -> Option<DataPayload> {
        let _span = span!(tracing::Level::DEBUG, "cache_check").entered();

        let state = self.mutex.lock().await;
        let mut hasher = DefaultHasher::new();
        for input in inputs {
            input.sha256_hash.hash(&mut hasher);
        }
        let input_hash = format!("{:x}", hasher.finish());

        let submap_key = (cache_key.clone(), input_hash.clone());

        let Some(submap) = state.map.get(&namespace.to_string()) else {
            debug!("No cache entry found");
            return None;
        };
        submap.get(&submap_key).cloned()
    }
}
