use std::sync::Arc;

use crate::{
    blob_store,
    executors::ExecutorManager,
    metrics::api_io_stats,
    state_store::{IndexifyState, kv::KVS},
};

#[derive(Clone)]
pub struct RouteState {
    pub indexify_state: Arc<IndexifyState>,
    pub blob_storage: Arc<blob_store::registry::BlobStorageRegistry>,
    pub kvs: Arc<KVS>,
    pub executor_manager: Arc<ExecutorManager>,
    pub metrics: Arc<api_io_stats::Metrics>,
}
