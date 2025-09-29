use std::{
    env,
    fmt::{Debug, Display},
    net::SocketAddr,
    time::Duration,
};

use anyhow::Result;
use figment::{
    providers::{Format, Serialized, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::{blob_store::BlobStorageConfig, executor_api::executor_api_pb};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorCatalogEntry {
    pub name: String,
    pub cpu_cores: u32,
    pub memory_gb: u64,
    pub disk_gb: u64,
    #[serde(default)]
    pub gpu_models: Vec<String>,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

impl Display for ExecutorCatalogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node: (name: {}, cpu_cores: {}, memory_gb: {}, disk_gb: {}, gpu_models: {:?}, labels: {:?})", self.name, self.cpu_cores, self.memory_gb, self.disk_gb, self.gpu_models, self.labels)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub dev: bool,
    pub state_store_path: String,
    pub listen_addr: String,
    pub listen_addr_grpc: String,
    pub blob_storage: BlobStorageConfig,
    pub kv_storage: BlobStorageConfig,
    pub telemetry: TelemetryConfig,
    pub executor_catalog: Vec<ExecutorCatalogEntry>,
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
            executor_catalog: Vec::new(),
            queue_size: 1,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TLSConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_bundle_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub dev: bool,
    pub hostname: String,
    pub server_address: String,
    pub grpc_server_address: String,
    pub function_uris: Vec<String>,
    pub executor_cache_path: String,
    pub monitoring_server_host: String,
    pub monitoring_server_port: u16,
    pub labels: Vec<String>,
    pub tls_config: Option<TLSConfig>,
    pub executor_version: String,
    pub telemetry: TelemetryConfig,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            dev: true,
            hostname: "localhost".to_string(),
            server_address: "127.0.0.1:8900".to_string(),
            grpc_server_address: "127.0.0.1:8901".to_string(),
            function_uris: Vec::new(),
            executor_cache_path: "~/.indexify/executor_cache".to_string(),
            monitoring_server_host: "127.0.0.1".to_string(),
            monitoring_server_port: 7000,
            labels: Vec::new(),
            tls_config: None,
            executor_version: env!("CARGO_PKG_VERSION").to_string(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

impl AgentConfig {
    pub fn from_path(path: &str) -> Result<AgentConfig> {
        let config_str = std::fs::read_to_string(path)?;
        let figment = Figment::from(Serialized::defaults(AgentConfig::default()));
        let config: AgentConfig = figment.merge(Yaml::string(&config_str)).extract()?;
        Ok(config)
    }

    pub fn labels_map(&self) -> std::collections::HashMap<String, String> {
        let mut labels = std::collections::HashMap::new();
        for kv in &self.labels {
            if let Some((k, v)) = kv.split_once('=') {
                labels.insert(k.to_string(), v.to_string());
            }
        }
        labels
    }

    pub fn allowed_functions(&self) -> Vec<executor_api_pb::AllowedFunction> {
        let mut out = Vec::new();
        for uri in &self.function_uris {
            // Expect <namespace>:<application>:<function>[:<version>]
            let parts: Vec<&str> = uri.split(':').collect();
            if parts.len() < 3 {
                continue;
            }
            let mut af = executor_api_pb::AllowedFunction::default();
            af.namespace = Some(parts[0].to_string());
            af.application_name = Some(parts[1].to_string());
            af.function_name = Some(parts[2].to_string());
            if parts.len() >= 4 {
                af.application_version = Some(parts[3].to_string());
            }
            out.push(af);
        }
        out
    }
}
