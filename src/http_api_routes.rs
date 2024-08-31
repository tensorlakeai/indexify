use std::sync::Arc;

use crate::{blob_storage::ContentReader, coordinator::Coordinator, data_manager::DataManager, metrics};

#[derive(Clone)]
pub struct NamespaceEndpointState {
    pub data_manager: Arc<DataManager>,
    pub coordinator: Arc<Coordinator>,
    pub content_reader: Arc<ContentReader>,
    pub registry: Arc<prometheus::Registry>,
    pub metrics: Arc<metrics::server::Metrics>,
}

pub fn create_routes(state: NamespaceEndpointState) -> Router {
    Router::new().route("/metrics", get(metrics_handler))
}