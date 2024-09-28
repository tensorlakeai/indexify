use std::{env, fmt::Debug, net::SocketAddr};

use anyhow::Result;
use blob_store::BlobStorageConfig;
use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub state_store_path: String,
    pub listen_addr: String,
    pub blob_storage: BlobStorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let state_store_path = env::current_dir().unwrap().join("indexify_storage/state");
        ServerConfig {
            state_store_path: state_store_path.to_str().unwrap().to_string(),
            listen_addr: "0.0.0.0:8900".to_string(),
            blob_storage: Default::default(),
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let config: ServerConfig = Figment::new().merge(Yaml::string(&config_str)).extract()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.blob_storage.s3.is_some() && self.blob_storage.disk.is_some() {
            return Err(anyhow::anyhow!(
                "cannot specify both s3 and disk blob storage"
            ));
        }
        if self.blob_storage.s3.is_none() && self.blob_storage.disk.is_none() {
            return Err(anyhow::anyhow!(
                "must specify one of s3 or disk blob storage"
            ));
        }
        if self.listen_addr.parse::<SocketAddr>().is_err() {
            return Err(anyhow::anyhow!(
                "invalid listen address: {}",
                self.listen_addr
            ));
        }
        Ok(())
    }
}
