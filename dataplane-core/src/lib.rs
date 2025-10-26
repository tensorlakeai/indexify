use std::sync::Arc;

use crate::{config::Config, heartbeat::HeartbeatService};

pub mod config;
pub mod executor_client;
mod heartbeat;
pub mod containers;

pub struct DataplaneService {
    heartbeat_service: Arc<heartbeat::HeartbeatService>,
}

impl DataplaneService {
    pub fn new(config: Config) -> Self {
        let heartbeat_service = Arc::new(HeartbeatService::new(Arc::new(config)));
        DataplaneService {heartbeat_service}
    }
}