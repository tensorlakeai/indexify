use std::{
    env,
    fmt::{Debug, Display},
    net::SocketAddr,
    time::Duration,
};

use anyhow::Result;
use figment::{
    Figment,
    providers::{Format, Serialized, Yaml},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{blob_store::BlobStorageConfig, state_store::driver::rocksdb::RocksDBConfig};

const LOCAL_ENV: &str = "local";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueBackend {
    AmazonSqs { queue_url: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub backend: QueueBackend,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuModel {
    pub name: String,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorCatalogEntry {
    pub name: String,
    pub cpu_cores: u32,
    pub memory_gb: u64,
    pub disk_gb: u64,
    #[serde(default)]
    pub gpu_model: Option<GpuModel>,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

impl Display for ExecutorCatalogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node: (name: {}, cpu_cores: {}, memory_gb: {}, disk_gb: {}, gpu_model: {:?}, labels: {:?})",
            self.name, self.cpu_cores, self.memory_gb, self.disk_gb, self.gpu_model, self.labels
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub env: String,
    pub state_store_path: String,
    pub rocksdb_config: RocksDBConfig,
    pub listen_addr: String,
    pub listen_addr_grpc: String,
    pub blob_storage: BlobStorageConfig,
    pub kv_storage: BlobStorageConfig,
    pub usage_queue: Option<QueueConfig>,
    pub telemetry: TelemetryConfig,
    pub executor_catalog: Vec<ExecutorCatalogEntry>,
    pub queue_size: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let state_store_path = env::current_dir().unwrap().join("indexify_storage/state");
        ServerConfig {
            env: LOCAL_ENV.to_string(),
            state_store_path: state_store_path.to_str().unwrap().to_string(),
            rocksdb_config: RocksDBConfig::default(),
            listen_addr: "0.0.0.0:8900".to_string(),
            listen_addr_grpc: "0.0.0.0:8901".to_string(),
            blob_storage: Default::default(),
            kv_storage: Default::default(),
            telemetry: TelemetryConfig::default(),
            executor_catalog: Vec::new(),
            queue_size: 1,
            usage_queue: None,
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let figment = Figment::from(Serialized::defaults(ServerConfig::default()));

        let config: ServerConfig = figment.merge(Yaml::string(&config_str)).extract()?;

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

    pub fn structured_logging(&self) -> bool {
        self.env != LOCAL_ENV
    }

    pub fn instance_id(&self) -> String {
        self.telemetry
            .instance_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", self.env, Uuid::new_v4()))
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
            instance_id: None,
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
