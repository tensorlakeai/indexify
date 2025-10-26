use std::sync::Arc;

use crate::config::Config;

pub struct HeartbeatService {
    config: Arc<Config>,
}

impl HeartbeatService {
    pub fn new(config: Arc<Config>) -> Self {
        HeartbeatService { config }
    }
}