use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use uuid::Uuid;

const LOCAL_ENV: &str = "local";
const DEFAULT_METRICS_INTERVAL_SECS: u64 = 5;

/// TLS configuration for authenticating with the gRPC server.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    /// Enable TLS for gRPC connections.
    #[serde(default)]
    pub enabled: bool,
    /// Path to the CA certificate file (PEM format).
    /// Used to verify the server's certificate.
    #[serde(default)]
    pub ca_cert_path: Option<String>,
    /// Path to the client certificate file (PEM format).
    /// Required for mutual TLS (mTLS) authentication.
    #[serde(default)]
    pub client_cert_path: Option<String>,
    /// Path to the client private key file (PEM format).
    /// Required for mutual TLS (mTLS) authentication.
    #[serde(default)]
    pub client_key_path: Option<String>,
    /// Domain name to use for TLS verification.
    /// If not specified, the domain from the server address will be used.
    #[serde(default)]
    pub domain_name: Option<String>,
}

impl TlsConfig {
    /// Validates the TLS configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // If client cert is provided, client key must also be provided
        if self.client_cert_path.is_some() != self.client_key_path.is_some() {
            return Err(anyhow::anyhow!(
                "Both client_cert_path and client_key_path must be provided for mTLS"
            ));
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TracingExporter {
    Stdout,
    Otlp,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DriverConfig {
    #[default]
    ForkExec,
    Docker {
        /// Docker daemon address. Supports:
        /// - Unix socket: `unix:///var/run/docker.sock` or `/var/run/docker.sock`
        /// - HTTP: `http://localhost:2375` or `tcp://localhost:2375`
        /// - HTTPS: `https://localhost:2376`
        ///
        /// If not specified, uses Docker's default socket location.
        #[serde(default)]
        address: Option<String>,
    },
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// Enable metrics export.
    #[serde(default)]
    pub enable_metrics: bool,
    /// OpenTelemetry collector grpc endpoint for traces and metrics.
    /// Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var or to
    /// localhost:4317 if empty.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Defines the exporter to use for tracing.
    /// If not specified, we won't export traces anywhere.
    #[serde(default)]
    pub tracing_exporter: Option<TracingExporter>,
    /// Metrics export interval in seconds. Defaults to 10 seconds.
    #[serde_inline_default(Duration::from_secs(DEFAULT_METRICS_INTERVAL_SECS))]
    #[serde(with = "duration_serde")]
    pub metrics_interval: Duration,
    /// Instance ID for this dataplane instance.
    #[serde(default)]
    pub instance_id: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enable_metrics: false,
            endpoint: None,
            tracing_exporter: None,
            metrics_interval: Duration::from_secs(DEFAULT_METRICS_INTERVAL_SECS),
            instance_id: None,
        }
    }
}

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

/// Configuration for the dataplane service.
#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataplaneConfig {
    /// Environment name (e.g., "local", "staging", "production").
    #[serde_inline_default(LOCAL_ENV.to_string())]
    pub env: String,
    /// Unique identifier for this executor.
    #[serde(default = "default_executor_id")]
    pub executor_id: String,
    /// The gRPC server address to connect to.
    #[serde_inline_default("http://localhost:8901".to_string())]
    pub server_addr: String,
    /// TLS configuration for authenticating with the gRPC server.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Telemetry configuration.
    #[serde(default)]
    pub telemetry: TelemetryConfig,
    /// Process driver configuration.
    #[serde(default)]
    pub driver: DriverConfig,
    /// Path to the state file for persisting container state across restarts.
    #[serde_inline_default("./dataplane-state.json".to_string())]
    pub state_file: String,
}

fn default_executor_id() -> String {
    Uuid::new_v4().to_string()
}

impl Default for DataplaneConfig {
    fn default() -> Self {
        DataplaneConfig {
            env: LOCAL_ENV.to_string(),
            executor_id: default_executor_id(),
            server_addr: "http://localhost:8901".to_string(),
            tls: TlsConfig::default(),
            telemetry: TelemetryConfig::default(),
            driver: DriverConfig::default(),
            state_file: "./dataplane-state.json".to_string(),
        }
    }
}

impl DataplaneConfig {
    pub fn from_path(path: &str) -> Result<DataplaneConfig> {
        let config_str = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&config_str)
    }

    fn from_yaml_str(config_str: &str) -> Result<DataplaneConfig> {
        let config: DataplaneConfig = serde_saphyr::from_str(config_str)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        self.tls.validate()?;
        Ok(())
    }

    pub fn structured_logging(&self) -> bool {
        self.env != LOCAL_ENV
    }

    pub fn instance_id(&self) -> String {
        self.telemetry
            .instance_id
            .clone()
            .unwrap_or_else(|| format!("dataplane-{}-{}", self.env, Uuid::new_v4()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DataplaneConfig::default();
        assert_eq!(config.env, "local");
        assert_eq!(config.server_addr, "http://localhost:8901");
        assert!(!config.tls.enabled);
    }

    #[test]
    fn test_parse_yaml_config() {
        let yaml = r#"
env: staging
server_addr: "https://indexify.example.com:8901"
tls:
  enabled: true
  ca_cert_path: "/etc/certs/ca.pem"
  client_cert_path: "/etc/certs/client.pem"
  client_key_path: "/etc/certs/client-key.pem"
  domain_name: "indexify.example.com"
telemetry:
  tracing_exporter: otlp
  endpoint: "http://otel-collector:4317"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.env, "staging");
        assert_eq!(config.server_addr, "https://indexify.example.com:8901");
        assert!(config.tls.enabled);
        assert_eq!(
            config.tls.ca_cert_path,
            Some("/etc/certs/ca.pem".to_string())
        );
    }

    #[test]
    fn test_invalid_mtls_config() {
        let yaml = r#"
env: staging
server_addr: "https://indexify.example.com:8901"
tls:
  enabled: true
  client_cert_path: "/etc/certs/client.pem"
"#;
        let result = DataplaneConfig::from_yaml_str(yaml);
        assert!(result.is_err());
    }
}
