use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    fmt::{Debug, Display},
    net::SocketAddr,
    time::Duration,
};

use anyhow::{Result, anyhow};
use saphyr::{LoadableYamlNode, Scalar, Yaml};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{blob_store::BlobStorageConfig, state_store::driver::rocksdb::RocksDBConfig};

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

impl TryFrom<&Yaml<'_>> for QueueConfig {
    type Error = anyhow::Error;

    fn try_from(value: &Yaml<'_>) -> Result<Self, Self::Error> {
        const BACKEND: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("backend")));
        let backend = value
            .as_mapping()
            .ok_or(anyhow::anyhow!("expected mapping for queue config"))?
            .get(&BACKEND)
            .ok_or(anyhow::anyhow!("expected 'backend' field in queue config"))?
            .as_mapping()
            .ok_or(anyhow::anyhow!(
                "expected mapping value for 'backend' field in queue config"
            ))?
            .front()
            .ok_or(anyhow::anyhow!("expected data in backend field config"))?;

        let (key, value) = backend;
        let key = key
            .as_str()
            .ok_or(anyhow::anyhow!("expected string key for backend field"))?;

        match key {
            "amazon_sqs" => {
                let mapping = value.as_mapping().ok_or(anyhow::anyhow!(
                    "expected mapping value for 'amazon_sqs' backend"
                ))?;

                const QUEUE_URL: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("queue_url")));
                let queue_url = mapping
                    .get(&QUEUE_URL)
                    .ok_or(anyhow::anyhow!(
                        "expected 'queue_url' field in 'amazon_sqs' backend"
                    ))?
                    .as_str()
                    .ok_or(anyhow::anyhow!(
                        "expected string value for 'queue_url' field in 'amazon_sqs' backend"
                    ))?;
                Ok(QueueConfig {
                    backend: QueueBackend::AmazonSqs {
                        queue_url: queue_url.to_string(),
                    },
                })
            }
            _ => Err(anyhow::anyhow!("unsupported backend: {key}")),
        }
    }
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

impl TryFrom<(usize, &Yaml<'_>)> for ExecutorCatalogEntry {
    type Error = anyhow::Error;

    fn try_from((index, value): (usize, &Yaml<'_>)) -> Result<Self, Self::Error> {
        let mapping = value.as_mapping().ok_or(anyhow::anyhow!(
            "expected mapping for executor catalog entry at index {index}"
        ))?;

        const NAME: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("name")));
        let name = mapping
            .get(&NAME)
            .and_then(|n| n.as_str())
            .ok_or(anyhow::anyhow!(
                "expected name field for executor catalog entry at index {index}",
            ))?
            .to_string();

        const CPU_CORES: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("cpu_cores")));
        let cpu_cores = mapping
            .get(&CPU_CORES)
            .and_then(|c| c.as_integer())
            .ok_or(anyhow::anyhow!(
                "expected cpu_cores field for executor catalog entry at index {index}"
            ))?
            .try_into()?;

        const MEMORY_GB: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("memory_gb")));
        let memory_gb = mapping
            .get(&MEMORY_GB)
            .and_then(|m| m.as_integer())
            .ok_or(anyhow::anyhow!(
                "expected memory_gb field for executor catalog entry at index {index}",
            ))?
            .try_into()?;

        const DISK_GB: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("disk_gb")));
        let disk_gb = mapping
            .get(&DISK_GB)
            .and_then(|d| d.as_integer())
            .ok_or(anyhow::anyhow!(
                "expected disk_gb field for executor catalog entry at index {index}"
            ))?
            .try_into()?;

        const GPU_MODEL: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("gpu_model")));
        let gpu_model = match mapping.get(&GPU_MODEL) {
            Some(gpu_model) => {
                let gpu_model = gpu_model.as_mapping().ok_or(anyhow::anyhow!(
                    "expected mapping for gpu_model field for executor catalog entry at index {index}"
                ))?;

                let name = gpu_model.get(&NAME).and_then(|n| n.as_str()).ok_or(anyhow::anyhow!(
                    "expected name field for gpu_model field for executor catalog entry at index {index}"
                ))?.to_string();

                const COUNT: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("count")));
                let count = gpu_model.get(&COUNT).and_then(|c| c.as_integer()).ok_or(anyhow::anyhow!(
                    "expected count field for gpu_model field for executor catalog entry at index {index}"
                ))?.try_into()?;

                Some(GpuModel { name, count })
            }
            None => None,
        };

        const LABELS: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("labels")));
        let labels = match mapping.get(&LABELS).and_then(|l| l.as_mapping()) {
            Some(labels) => {
                labels.iter()
                .enumerate()
                .map(|(label_index, (k, v))| {
                    let key = k.as_str().ok_or(anyhow::anyhow!("expected string key for label at index {label_index} in executor catalog entry at index {index}"))?;
                    let value = v.as_str().ok_or(anyhow::anyhow!("expected string value for label at index {label_index} in executor catalog entry at index {index}"))?;
                    Ok::<(String, String), anyhow::Error>((key.to_string(), value.to_string()))
                })
                .collect::<Result<HashMap<String, String>, _>>()?
            }
            None => HashMap::default(),
        };

        Ok(ExecutorCatalogEntry {
            name,
            cpu_cores,
            memory_gb,
            disk_gb,
            gpu_model,
            labels,
        })
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
    pub cloud_events: Option<CloudEventsConfig>,
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
            cloud_events: None,
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<ServerConfig> {
        let config_str = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&config_str)
    }

    pub fn from_yaml_str(config_str: &str) -> Result<ServerConfig> {
        let config_docs = Yaml::load_from_str(config_str)?;
        let Some(config_doc) = config_docs.first() else {
            return Ok(ServerConfig::default());
        };

        let config: ServerConfig = config_doc.try_into()?;

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

impl TryFrom<&Yaml<'_>> for ServerConfig {
    type Error = anyhow::Error;

    fn try_from(yaml: &Yaml<'_>) -> Result<Self, Self::Error> {
        let mapping = yaml
            .as_mapping()
            .ok_or(anyhow::anyhow!("expected a mapping"))?;
        let mut config = Self::default();

        const ENV: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("env")));
        if let Some(env) = mapping.get(&ENV) {
            config.env = env
                .as_str()
                .ok_or(anyhow::anyhow!("expected a string for env"))?
                .to_string();
        }

        const STATE_STORE_PATH: Yaml =
            Yaml::Value(Scalar::String(Cow::Borrowed("state_store_path")));
        if let Some(state_store_path) = mapping.get(&STATE_STORE_PATH) {
            config.state_store_path = state_store_path
                .as_str()
                .ok_or(anyhow::anyhow!("expected a string for state_store_path"))?
                .to_string();
        }

        const ROCKSDB_CONFIG: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("rocksdb_config")));
        if let Some(rocksdb_config) = mapping.get(&ROCKSDB_CONFIG) {
            config.rocksdb_config = rocksdb_config.try_into()?;
        }

        const LISTEN_ADDR: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("listen_addr")));
        if let Some(listen_addr) = mapping.get(&LISTEN_ADDR) {
            config.listen_addr = listen_addr
                .as_str()
                .ok_or(anyhow::anyhow!("expected a string for listen_addr"))?
                .to_string();
        }

        const LISTEN_ADDR_GRPC: Yaml =
            Yaml::Value(Scalar::String(Cow::Borrowed("listen_addr_grpc")));
        if let Some(listen_addr_grpc) = mapping.get(&LISTEN_ADDR_GRPC) {
            config.listen_addr_grpc = listen_addr_grpc
                .as_str()
                .ok_or(anyhow::anyhow!("expected a string for listen_addr_grpc"))?
                .to_string();
        }

        const BLOB_STORAGE: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("blob_storage")));
        if let Some(blob_storage) = mapping.get(&BLOB_STORAGE) {
            config.blob_storage = ("blob_storage", blob_storage).try_into()?;
        }

        const KV_STORAGE: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("kv_storage")));
        if let Some(kv_storage) = mapping.get(&KV_STORAGE) {
            config.kv_storage = ("kv_storage", kv_storage).try_into()?;
        }

        const USAGE_QUEUE: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("usage_queue")));
        if let Some(usage_queue) = mapping.get(&USAGE_QUEUE) {
            config.usage_queue = Some(usage_queue.try_into()?);
        }

        const TELEMETRY: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("telemetry")));
        if let Some(telemetry) = mapping.get(&TELEMETRY) {
            config.telemetry = telemetry.try_into()?;
        }

        const EXECUTOR_CATALOG: Yaml =
            Yaml::Value(Scalar::String(Cow::Borrowed("executor_catalog")));
        if let Some(executor_catalog) = mapping.get(&EXECUTOR_CATALOG) {
            let executor_catalog = executor_catalog
                .as_sequence()
                .ok_or(anyhow::anyhow!("expected a sequence for executor_catalog"))?
                .iter()
                .enumerate()
                .map(|(index, entry)| {
                    let entry: ExecutorCatalogEntry = (index, entry).try_into()?;
                    Ok(entry)
                })
                .collect::<Result<Vec<_>>>()?;
            config.executor_catalog = executor_catalog;
        }

        const QUEUE_SIZE: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("queue_size")));
        if let Some(queue_size) = mapping.get(&QUEUE_SIZE) {
            let queue_size = queue_size
                .as_integer()
                .ok_or(anyhow::anyhow!("expected an integer for queue_size"))?;
            config.queue_size = queue_size.try_into()?;
        }

        const CLOUD_EVENTS: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("cloud_events")));
        if let Some(cloud_events) = mapping.get(&CLOUD_EVENTS) {
            config.cloud_events = Some(cloud_events.try_into()?);
        }

        Ok(config)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TracingExporter {
    Stdout,
    Otlp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    // Enable metrics.
    pub enable_metrics: bool,
    // OpenTelemetry collector grpc endpoint for both traces and metrics.
    // If specified, both traces and metrics will be sent to this endpoint.
    // Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var or to localhost:4317 if empty.
    pub endpoint: Option<String>,
    // Defines the exporter to use for tracing.
    // If not specified, we won't export traces anywhere.
    pub tracing_exporter: Option<TracingExporter>,
    // Metrics export interval. Defaults to 10 seconds.
    #[serde(with = "duration_serde")]
    pub metrics_interval: Duration,
    // Optional path to write local logs to a rotating file.
    pub local_log_file: Option<String>,
    // Instance ID for this Indexify server instance.
    // Used as a metric attribute "indexify.instance.id".
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

impl TryFrom<&Yaml<'_>> for TelemetryConfig {
    type Error = anyhow::Error;

    fn try_from(value: &Yaml<'_>) -> Result<Self, Self::Error> {
        let mapping = value
            .as_mapping()
            .ok_or(anyhow!("expected mapping for telemetry"))?;
        let mut config = Self::default();

        const ENABLE_METRICS: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("enable_metrics")));
        if let Some(enable_metrics) = mapping.get(&ENABLE_METRICS) {
            config.enable_metrics = enable_metrics
                .as_bool()
                .ok_or_else(|| anyhow!("Expected boolean value for 'enable_metrics'"))?;
        }

        const ENDPOINT: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("endpoint")));
        if let Some(endpoint) = mapping.get(&ENDPOINT) {
            config.endpoint = Some(
                endpoint
                    .as_str()
                    .ok_or_else(|| anyhow!("Expected string value for 'endpoint'"))?
                    .to_string(),
            );
        }

        const TRACING_EXPORTER: Yaml =
            Yaml::Value(Scalar::String(Cow::Borrowed("tracing_exporter")));
        if let Some(tracing_exporter) = mapping.get(&TRACING_EXPORTER) {
            let tracing_exporter = tracing_exporter
                .as_str()
                .ok_or_else(|| anyhow!("Expected string value for 'tracing_exporter'"))?;

            let tracing_exporter = match tracing_exporter {
                "stdout" => TracingExporter::Stdout,
                "otlp" => TracingExporter::Otlp,
                _ => return Err(anyhow!("Invalid value for 'tracing_exporter'")),
            };

            config.tracing_exporter = Some(tracing_exporter);
        }

        const METRICS_INTERVAL: Yaml =
            Yaml::Value(Scalar::String(Cow::Borrowed("metrics_interval")));
        if let Some(metrics_interval) = mapping.get(&METRICS_INTERVAL) {
            let metrics_interval: u64 = metrics_interval
                .as_integer()
                .ok_or_else(|| anyhow!("Expected integer value for 'metrics_interval'"))?
                .try_into()?;

            config.metrics_interval = Duration::from_secs(metrics_interval);
        }

        const LOCAL_LOG_FILE: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("local_log_file")));
        if let Some(local_log_file) = mapping.get(&LOCAL_LOG_FILE) {
            config.local_log_file = Some(
                local_log_file
                    .as_str()
                    .ok_or_else(|| anyhow!("Expected string value for 'local_log_file'"))?
                    .to_string(),
            );
        }

        const INSTANCE_ID: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("instance_id")));
        if let Some(instance_id) = mapping.get(&INSTANCE_ID) {
            config.instance_id = Some(
                instance_id
                    .as_str()
                    .ok_or_else(|| anyhow!("Expected string value for 'instance_id'"))?
                    .to_string(),
            );
        }

        Ok(config)
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

impl TryFrom<&Yaml<'_>> for CloudEventsConfig {
    type Error = anyhow::Error;

    fn try_from(value: &Yaml) -> Result<Self, Self::Error> {
        const ENDPOINT: Yaml = Yaml::Value(Scalar::String(Cow::Borrowed("endpoint")));

        let endpoint = value
            .as_mapping()
            .ok_or(anyhow::anyhow!("expected mapping for cloud events"))?
            .get(&ENDPOINT)
            .ok_or_else(|| anyhow!("missing 'endpoint' field"))?
            .as_str()
            .ok_or_else(|| anyhow!("Expected string value for 'endpoint'"))?
            .to_string();

        Ok(CloudEventsConfig { endpoint })
    }
}
