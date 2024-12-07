use std::{env, fmt::Debug, net::SocketAddr};

use anyhow::Result;
use blob_store::BlobStorageConfig;
use figment::{
    providers::{Format, Serialized, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub dev: bool,
    pub state_store_path: String,
    pub listen_addr: String,
    pub blob_storage: BlobStorageConfig,
    pub tracing: TracingConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let state_store_path = env::current_dir().unwrap().join("indexify_storage/state");
        ServerConfig {
            dev: false,
            state_store_path: state_store_path.to_str().unwrap().to_string(),
            listen_addr: "0.0.0.0:8900".to_string(),
            blob_storage: Default::default(),
            tracing: TracingConfig::default(),
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let config: ServerConfig = Figment::from(Serialized::defaults(ServerConfig::default()))
            .merge(Yaml::string(&config_str))
            .extract()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.listen_addr.parse::<SocketAddr>().is_err() {
            return Err(anyhow::anyhow!(
                "invalid listen address: {}",
                self.listen_addr
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TracingConfig {
    // Enable tracing.
    pub enabled: bool,
    // OpenTelemetry collector grpc endpoint. Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var
    // or to localhost:4317 if empty.
    pub endpoint: Option<String>,
}
