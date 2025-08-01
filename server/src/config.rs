use std::{env, fmt::Debug, net::SocketAddr, time::Duration};

use anyhow::Result;
use figment::{
    providers::{Format, Serialized, Toml, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::{blob_store::BlobStorageConfig, data_model};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub dev: bool,
    pub state_store_path: String,
    pub listen_addr: String,
    pub listen_addr_grpc: String,
    pub blob_storage: BlobStorageConfig,
    pub kv_storage: BlobStorageConfig,
    pub telemetry: TelemetryConfig,
    pub executor: ExecutorConfig,
    pub labels: Vec<std::collections::HashMap<String, String>>,
    pub queue_size: u32,
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
            kv_storage: Default::default(),
            telemetry: TelemetryConfig::default(),
            executor: ExecutorConfig::default(),
            labels: Vec::new(),
            queue_size: 2,
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let figment = Figment::from(Serialized::defaults(ServerConfig::default()));

        let config: ServerConfig = if path.ends_with(".toml") {
            figment.merge(Toml::string(&config_str))
        } else {
            // Default to YAML for .yaml, .yml, or any other extension
            figment.merge(Yaml::string(&config_str))
        }
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
        self.executor.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    // Enable tracing.
    pub enable_tracing: bool,
    // Enable metrics.
    pub enable_metrics: bool,
    // OpenTelemetry collector grpc endpoint for both traces and metrics.
    // If specified, both traces and metrics will be sent to this endpoint.
    // Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var or to localhost:4317 if empty.
    pub endpoint: Option<String>,
    // Metrics export interval. Defaults to 10 seconds.
    #[serde(with = "duration_serde")]
    pub metrics_interval: Duration,
    // Optional path to write local logs to a rotating file.
    pub local_log_file: Option<String>,
    // List of targets and their log levels for local logging.
    // Format: {"target_name": "log_level"}, e.g., {"scheduler": "debug"}
    #[serde(default)]
    pub local_log_targets: std::collections::HashMap<String, String>,
    // Instance ID for this Indexify server instance.
    // Used as a metric attribute "indexify.instance.id".
    pub instance_id: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enable_tracing: false,
            enable_metrics: false,
            endpoint: None,
            metrics_interval: Duration::from_secs(10),
            local_log_file: None,
            local_log_targets: std::collections::HashMap::new(),
            instance_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub max_cpus_per_function: u32,
    pub max_memory_gb_per_function: u32,
    pub max_disk_gb_per_function: u32,
    pub max_gpus_per_function: u32,
    pub allowed_gpu_models: Vec<String>,
}

impl ExecutorConfig {
    fn validate(&self) -> Result<()> {
        // Just check for impossible values.
        if self.max_cpus_per_function == 0 || self.max_cpus_per_function > 1024 {
            return Err(anyhow::anyhow!(
                "max_cpus_per_function must be greater than 0 and less than or equal 1024"
            ));
        }
        if self.max_memory_gb_per_function == 0 || self.max_memory_gb_per_function > 100 * 1024 {
            return Err(anyhow::anyhow!(
                "max_memory_gb_per_function must be greater than 0 and less than or equal 100 TB"
            ));
        }
        if self.max_disk_gb_per_function > 500 * 1024 {
            return Err(anyhow::anyhow!(
                "max_disk_gb_per_function must be greater than 0 and less than or equal 500 TB"
            ));
        }
        // Allow 0 GPUs for CPU-only clusters and 0 ephimeral disk for diskless
        // clusters.
        for model in &self.allowed_gpu_models {
            if !data_model::ALL_GPU_MODELS.contains(&model.as_str()) {
                return Err(anyhow::anyhow!(
                    "invalid GPU model: {:?}, supported GPU models: {:?}",
                    model,
                    data_model::ALL_GPU_MODELS
                ));
            }
        }
        Ok(())
    }
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

// Serde module for Duration serialization/deserialization
mod duration_serde {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let seconds = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(seconds))
    }
}
