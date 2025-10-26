use crate::config::Config;

pub mod config;
pub mod executor_client;

pub struct DataplaneService {
}

impl DataplaneService {
    pub fn new(_config: Config) -> Self {
        DataplaneService {}
    }
}