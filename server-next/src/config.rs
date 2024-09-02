use anyhow::Result;
use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStorageConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    pub s3: Option<S3Config>,
    pub disk: Option<DiskStorageConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub state_store_path: String,
    pub listen_addr: String,
    pub blob_storage: BlobStorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            state_store_path: "indexify_storage/state".to_string(),
            listen_addr: "0.0.0.0:8900".to_string(),
            blob_storage: BlobStorageConfig {
                s3: None,
                disk: Some(DiskStorageConfig {
                    path: "indexify_storage/blobs".to_string(),
                }),
            },
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let config: ServerConfig = Figment::new().merge(Yaml::string(&config_str)).extract()?;
        Ok(config)
    }
}
