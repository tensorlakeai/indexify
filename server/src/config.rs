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
    pub listen_addr_grpc: String,
    pub blob_storage: BlobStorageConfig,
    pub tracing: TracingConfig,
    pub executor: ExecutorConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let state_store_path = env::current_dir().unwrap().join("indexify_storage/state");
        ServerConfig {
            dev: false,
            state_store_path: state_store_path.to_str().unwrap().to_string(),
            listen_addr: "0.0.0.0:8900".to_string(),
            listen_addr_grpc: "0.0.0.0:8901".to_string(),
            blob_storage: Default::default(),
            tracing: TracingConfig::default(),
            executor: ExecutorConfig::default(),
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
        if self.listen_addr_grpc.parse::<SocketAddr>().is_err() {
            return Err(anyhow::anyhow!(
                "invalid listen address grpc: {}",
                self.listen_addr_grpc
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub max_cpus_per_function: u32,
    pub max_memory_gb_per_function: u32,
    pub max_disk_gb_per_function: u32,
    pub max_gpus_per_function: u32,
    pub allowed_gpu_models: Vec<String>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        ExecutorConfig {
            max_cpus_per_function: 216,          // 90% of 240
            max_memory_gb_per_function: 1620,    // 90% of 1800 GiB
            max_disk_gb_per_function: 20 * 1024, // 90% of 22 TiB
            max_gpus_per_function: 8,
            allowed_gpu_models: data_model::ALL_GPU_MODELS
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}
