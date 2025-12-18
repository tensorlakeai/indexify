use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    net::SocketAddr,
    path::Path,
    time::Duration,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use uuid::Uuid;

use crate::{blob_store::BlobStorageConfig, state_store::driver::ConnectionOptions};

const LOCAL_ENV: &str = "local";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub labels: HashMap<String, String>,
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

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde_inline_default(LOCAL_ENV.to_string())]
    pub env: String,
    pub driver_config: ConnectionOptions,
    #[serde_inline_default("0.0.0.0:8900".to_string())]
    pub listen_addr: String,
    #[serde_inline_default("0.0.0.0:8901".to_string())]
    pub listen_addr_grpc: String,
    #[serde(default)]
    pub blob_storage: BlobStorageConfig,
    #[serde(default)]
    pub kv_storage: BlobStorageConfig,
    #[serde(default)]
    pub usage_queue: Option<QueueConfig>,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    #[serde(default)]
    pub executor_catalog: Vec<ExecutorCatalogEntry>,
    #[serde_inline_default(1)]
    pub queue_size: u32,
    #[serde(default)]
    pub cloud_events: Option<CloudEventsConfig>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            env: LOCAL_ENV.to_string(),
            driver_config: ConnectionOptions::default(),
            listen_addr: "0.0.0.0:8900".to_string(),
            listen_addr_grpc: "0.0.0.0:8901".to_string(),
            blob_storage: Default::default(),
            kv_storage: Default::default(),
            telemetry: Default::default(),
            executor_catalog: Vec::new(),
            queue_size: 1,
            usage_queue: None,
            cloud_events: None,
        }
    }
}

impl ServerConfig {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path.as_ref())?;
        Self::from_yaml_str(&config_str)
    }

    fn from_yaml_str(config_str: &str) -> Result<ServerConfig> {
        let config: ServerConfig = serde_saphyr::from_str(config_str)?;

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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TracingExporter {
    Stdout,
    Otlp,
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    // Enable metrics.
    #[serde(default)]
    pub enable_metrics: bool,
    // OpenTelemetry collector grpc endpoint for both traces and metrics.
    // If specified, both traces and metrics will be sent to this endpoint.
    // Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var or to localhost:4317 if empty.
    #[serde(default)]
    pub endpoint: Option<String>,
    // Defines the exporter to use for tracing.
    // If not specified, we won't export traces anywhere.
    #[serde(default)]
    pub tracing_exporter: Option<TracingExporter>,
    // Metrics export interval. Defaults to 10 seconds.
    #[serde(with = "duration_serde")]
    #[serde_inline_default(Duration::from_secs(10))]
    pub metrics_interval: Duration,
    // Optional path to write local logs to a rotating file.
    #[serde(default)]
    pub local_log_file: Option<String>,
    // Instance ID for this Indexify server instance.
    // Used as a metric attribute "indexify.instance.id".
    #[serde(default)]
    pub instance_id: Option<String>,
}

impl TelemetryConfig {
    pub fn tracing_enabled(&self) -> bool {
        self.tracing_exporter.is_some()
    }
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enable_metrics: false,
            endpoint: None,
            tracing_exporter: None,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CloudEventsConfig {
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use crate::{config::ServerConfig, state_store::driver::ConnectionOptions};

    #[test]
    pub fn should_parse_sample_config() {
        let config_yaml = include_str!("../sample_config.yaml");
        let config = ServerConfig::from_yaml_str(config_yaml).expect("unable to parse from yaml");

        assert_eq!("local", config.env);
        assert_eq!(3, config.executor_catalog.len());
        let ConnectionOptions::RocksDB(driver) = config.driver_config else {
            panic!("Unexpected driver type");
        };
        assert_eq!("indexify_server_state", driver.path.to_string_lossy());
    }
}
