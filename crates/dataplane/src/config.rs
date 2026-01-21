use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use uuid::Uuid;

const LOCAL_ENV: &str = "local";

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
        #[serde(default)]
        socket_path: Option<String>,
    },
}

#[serde_inline_default]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// OpenTelemetry collector grpc endpoint for traces.
    /// Defaults to using OTEL_EXPORTER_OTLP_ENDPOINT env var or to
    /// localhost:4317 if empty.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Defines the exporter to use for tracing.
    /// If not specified, we won't export traces anywhere.
    #[serde(default)]
    pub tracing_exporter: Option<TracingExporter>,
    /// Instance ID for this dataplane instance.
    #[serde(default)]
    pub instance_id: Option<String>,
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
