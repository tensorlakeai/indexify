use std::sync::Arc;

use crate::{
    blob_store,
    config::ServerConfig,
    executors::ExecutorManager,
    metrics::api_io_stats,
    state_store::{kv::KVS, IndexifyState},
};

#[derive(Clone)]
pub struct RouteState {
    pub config: Arc<ServerConfig>,
    pub indexify_state: Arc<IndexifyState>,
    pub blob_storage: Arc<blob_store::BlobStorage>,
    pub kvs: Arc<KVS>,
    pub executor_manager: Arc<ExecutorManager>,
    pub metrics: Arc<api_io_stats::Metrics>,
}
