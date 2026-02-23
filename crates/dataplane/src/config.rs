use std::{path::PathBuf, time::Duration};

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
        /// - Unix socket: `unix:///var/run/docker.sock` or
        ///   `/var/run/docker.sock`
        /// - HTTP: `http://localhost:2375` or `tcp://localhost:2375`
        /// - HTTPS: `https://localhost:2376`
        ///
        /// If not specified, uses Docker's default socket location.
        #[serde(default)]
        address: Option<String>,
        /// OCI runtime to use for containers (e.g., "runsc" for gVisor).
        /// If not specified, uses Docker's default runtime (typically runc).
        #[serde(default)]
        runtime: Option<String>,
        /// Docker network mode for containers (e.g., "bridge", "host",
        /// or a custom network name).
        /// If not specified, uses Docker's default network mode.
        #[serde(default)]
        network: Option<String>,
        /// Volume bind mounts for function executor containers.
        /// Format: "host_path:container_path" or "host_path:container_path:ro".
        /// Useful for mounting the blob store directory when using local
        /// filesystem storage.
        #[serde(default)]
        binds: Vec<String>,
        /// Root directory for the runsc (gVisor) container state.
        /// This is the `--root` flag passed to `runsc` commands.
        /// Default: `/var/run/docker/runtime-runc/moby`.
        #[serde(default)]
        runsc_root: Option<String>,
        /// Local directory for storing gVisor snapshot overlay tars.
        /// Restored overlay tars are written here and referenced by the
        /// `dev.gvisor.tar.rootfs.upper` Docker annotation at container start.
        /// Default: `/tmp/indexify-snapshots`.
        #[serde(default)]
        snapshot_local_dir: Option<String>,
    },
    #[cfg(feature = "firecracker")]
    Firecracker {
        /// Path to firecracker binary. Default: "firecracker" (PATH lookup).
        #[serde(default)]
        firecracker_binary: Option<String>,
        /// Path to Linux kernel image (vmlinux).
        kernel_image_path: String,
        /// Per-VM COW file size in bytes. Default: 1 GiB.
        #[serde(default)]
        default_rootfs_size_bytes: Option<u64>,
        /// Path to base guest OS rootfs ext4 image.
        base_rootfs_image: String,
        /// CNI network name (matches conflist "name" field).
        cni_network_name: String,
        /// CNI plugin binaries directory. Default: "/opt/cni/bin".
        #[serde(default)]
        cni_bin_path: Option<String>,
        /// Guest gateway IP (e.g., "192.168.30.1").
        guest_gateway: String,
        /// Guest netmask. Default: "255.255.255.0".
        #[serde(default)]
        guest_netmask: Option<String>,
        /// Default vCPUs per VM. Default: 2.
        #[serde(default)]
        default_vcpu_count: Option<u32>,
        /// Default memory in MiB per VM. Default: 512.
        #[serde(default)]
        default_memory_mib: Option<u64>,
        /// Directory for FC sockets and VM metadata.
        /// Default: "/var/lib/indexify/firecracker".
        #[serde(default)]
        state_dir: Option<String>,
        /// Directory for VM log files.
        /// Default: "/var/log/indexify/firecracker".
        #[serde(default)]
        log_dir: Option<String>,
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

/// Configuration for upstream (container) connections.
/// These settings help prevent "connection prematurely closed" errors.
#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamConfig {
    /// Idle connection timeout in seconds. Connections idle longer than this
    /// will be closed. Set slightly lower than upstream timeout (usually 60s)
    /// to ensure we close before the upstream does.
    #[serde_inline_default(55)]
    pub idle_timeout_secs: u64,

    /// TCP keepalive idle time in seconds before sending probes.
    #[serde_inline_default(30)]
    pub keepalive_idle_secs: u64,

    /// TCP keepalive probe interval in seconds.
    #[serde_inline_default(10)]
    pub keepalive_interval_secs: u64,

    /// Number of TCP keepalive probes before giving up.
    #[serde_inline_default(3)]
    pub keepalive_count: usize,

    /// Connection establishment timeout in seconds.
    #[serde_inline_default(10)]
    pub connection_timeout_secs: u64,

    /// Read operation timeout in seconds.
    #[serde_inline_default(60)]
    pub read_timeout_secs: u64,

    /// Write operation timeout in seconds.
    #[serde_inline_default(60)]
    pub write_timeout_secs: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            idle_timeout_secs: 55,
            keepalive_idle_secs: 30,
            keepalive_interval_secs: 10,
            keepalive_count: 3,
            connection_timeout_secs: 10,
            read_timeout_secs: 60,
            write_timeout_secs: 60,
        }
    }
}

/// Configuration for the HTTP proxy server (header-based routing).
/// Accepts plaintext HTTP from the sandbox-proxy and routes to containers
/// based on the Tensorlake-Sandbox-Id header.
#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpProxyConfig {
    /// Port to listen on for HTTP connections.
    #[serde_inline_default(8095)]
    pub port: u16,
    /// Listen address for the HTTP proxy server.
    #[serde_inline_default("0.0.0.0".to_string())]
    pub listen_addr: String,
    /// Address to advertise to the server (host:port).
    /// This is the address that sandbox-proxy will connect to.
    /// If not set, uses the system hostname with the configured port.
    #[serde(default)]
    pub advertise_address: Option<String>,
    /// Upstream connection settings for container connections.
    #[serde(default)]
    pub upstream: UpstreamConfig,
}

impl Default for HttpProxyConfig {
    fn default() -> Self {
        Self {
            port: 8095,
            listen_addr: "0.0.0.0".to_string(),
            advertise_address: None,
            upstream: UpstreamConfig::default(),
        }
    }
}

impl HttpProxyConfig {
    /// Get the socket address to bind to.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.listen_addr, self.port)
    }

    /// Get the address to advertise to the server.
    /// Returns advertise_address if set, otherwise hostname:port.
    pub fn get_advertise_address(&self) -> String {
        if let Some(addr) = &self.advertise_address {
            addr.clone()
        } else {
            let hostname = hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "localhost".to_string());
            format!("{}:{}", hostname, self.port)
        }
    }
}

const DEFAULT_MONITORING_PORT: u16 = 8100;

/// Configuration for the HTTP monitoring server.
#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Port to listen on for HTTP monitoring requests.
    #[serde_inline_default(DEFAULT_MONITORING_PORT)]
    pub port: u16,
    /// Listen address for the monitoring server.
    #[serde_inline_default("0.0.0.0".to_string())]
    pub listen_addr: String,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_MONITORING_PORT,
            listen_addr: "0.0.0.0".to_string(),
        }
    }
}

impl MonitoringConfig {
    /// Get the socket address to bind to.
    pub fn socket_addr(&self) -> String {
        format!("{}:{}", self.listen_addr, self.port)
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
    /// Directory for all dataplane state files (container state, driver
    /// metadata, logs). Defaults to `./dataplane-state/` in the current
    /// working directory.
    /// The container state file is written as `{state_dir}/state.json`,
    /// and driver-specific subdirectories (e.g. `{state_dir}/firecracker/`)
    /// are created automatically unless the driver config overrides them.
    #[serde_inline_default("./dataplane-state".to_string())]
    pub state_dir: String,
    /// HTTP proxy server configuration (header-based routing).
    /// Receives requests from sandbox-proxy with X-Tensorlake-Sandbox-Id
    /// header.
    #[serde(default)]
    pub http_proxy: HttpProxyConfig,
    /// Path where the daemon binary will be extracted.
    /// Defaults to /tmp/indexify-container-daemon.
    #[serde(default)]
    pub daemon_binary_extract_path: Option<String>,
    /// Function executor configuration.
    #[serde(default)]
    pub function_executor: FunctionExecutorConfig,
    /// Function allowlist: only accept allocations for these functions.
    /// Format: "namespace:application:function" or
    /// "namespace:application:function:version".
    /// Empty list means accept all functions (default).
    #[serde(default)]
    pub function_allowlist: Vec<String>,
    /// Labels to advertise to the server.
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
    /// HTTP monitoring server configuration.
    #[serde(default)]
    pub monitoring: MonitoringConfig,
    /// Override probed host resources.
    #[serde(default)]
    pub resource_overrides: Option<ResourceOverrides>,
    /// Default container image for function containers.
    /// Used as a fallback by the default image resolver when no external
    /// image resolution service is configured.
    #[serde(default)]
    pub default_function_image: Option<String>,
    /// Snapshot storage URI for container filesystem snapshots.
    /// When set, enables snapshot create/restore via DockerSnapshotter.
    /// Examples: "s3://my-bucket/snapshots", "file:///var/data/snapshots".
    #[serde(default)]
    pub snapshot_storage_uri: Option<String>,
}

/// Resource overrides to replace probed host resources.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceOverrides {
    /// Override CPU count.
    #[serde(default)]
    pub cpu_count: Option<u32>,
    /// Override total memory.
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    /// Override total disk.
    #[serde(default)]
    pub disk_bytes: Option<u64>,
}

/// Configuration for function executor mode (subprocess-based).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionExecutorConfig {
    /// Path to cache application code. Defaults to /tmp/indexify_code_cache.
    #[serde(default = "default_code_cache_path")]
    pub code_cache_path: String,
    /// Blob store URL (e.g., "s3://bucket" or "file:///path").
    /// If not set, uses local filesystem.
    #[serde(default)]
    pub blob_store_url: Option<String>,
    /// Path to the function-executor binary.
    /// If not set, searches PATH.
    #[serde(default)]
    pub fe_binary_path: Option<String>,
}

fn default_code_cache_path() -> String {
    "/tmp/indexify_code_cache".to_string()
}

impl Default for FunctionExecutorConfig {
    fn default() -> Self {
        Self {
            code_cache_path: default_code_cache_path(),
            blob_store_url: None,
            fe_binary_path: None,
        }
    }
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
            state_dir: "./dataplane-state".to_string(),
            http_proxy: HttpProxyConfig::default(),
            daemon_binary_extract_path: None,
            function_executor: FunctionExecutorConfig::default(),
            function_allowlist: Vec::new(),
            labels: std::collections::HashMap::new(),
            monitoring: MonitoringConfig::default(),
            resource_overrides: None,
            default_function_image: None,
            snapshot_storage_uri: None,
        }
    }
}

impl DataplaneConfig {
    pub fn from_path(path: &str) -> Result<DataplaneConfig> {
        let config_str = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&config_str)
    }

    fn from_yaml_str(config_str: &str) -> Result<DataplaneConfig> {
        let mut config: DataplaneConfig = serde_saphyr::from_str(config_str)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&mut self) -> Result<()> {
        self.tls.validate()?;

        // Validate server_addr has a scheme
        if !self.server_addr.starts_with("http://") && !self.server_addr.starts_with("https://") {
            return Err(anyhow::anyhow!(
                "server_addr must include a scheme (http:// or https://), got: {}",
                self.server_addr
            ));
        }

        Ok(())
    }

    /// Path to the container state file, derived from `state_dir`.
    pub fn state_file_path(&self) -> PathBuf {
        PathBuf::from(&self.state_dir).join("state.json")
    }

    /// State directory for the Firecracker driver.
    /// Uses the driver's explicit `state_dir` if set, otherwise
    /// `{state_dir}/firecracker/`.
    #[cfg(feature = "firecracker")]
    pub fn firecracker_state_dir(&self) -> PathBuf {
        if let DriverConfig::Firecracker { state_dir, .. } = &self.driver {
            if let Some(dir) = state_dir {
                return PathBuf::from(dir);
            }
        }
        PathBuf::from(&self.state_dir).join("firecracker")
    }

    /// Log directory for the Firecracker driver.
    /// Uses the driver's explicit `log_dir` if set, otherwise
    /// `{state_dir}/firecracker/logs/`.
    #[cfg(feature = "firecracker")]
    pub fn firecracker_log_dir(&self) -> PathBuf {
        if let DriverConfig::Firecracker { log_dir, .. } = &self.driver {
            if let Some(dir) = log_dir {
                return PathBuf::from(dir);
            }
        }
        PathBuf::from(&self.state_dir)
            .join("firecracker")
            .join("logs")
    }

    pub fn structured_logging(&self) -> bool {
        self.env != LOCAL_ENV
    }

    /// Parse function_allowlist strings into AllowedFunction protos.
    /// Format: "namespace:application:function" or
    /// "namespace:application:function:version".
    pub fn parse_allowed_functions(&self) -> Vec<proto_api::executor_api_pb::AllowedFunction> {
        self.function_allowlist
            .iter()
            .filter_map(|uri| {
                let tokens: Vec<&str> = uri.split(':').collect();
                if tokens.len() < 3 || tokens.len() > 4 {
                    tracing::warn!(
                        uri = %uri,
                        "Invalid function URI, expected namespace:application:function[:version]"
                    );
                    return None;
                }
                Some(proto_api::executor_api_pb::AllowedFunction {
                    namespace: Some(tokens[0].to_string()),
                    application_name: Some(tokens[1].to_string()),
                    function_name: Some(tokens[2].to_string()),
                    application_version: tokens.get(3).map(|v| v.to_string()),
                })
            })
            .collect()
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
        let mut config = DataplaneConfig::default();
        assert_eq!(config.env, "local");
        assert_eq!(config.server_addr, "http://localhost:8901");
        assert!(!config.tls.enabled);
        assert!(config.validate().is_ok());
        // HTTP proxy has sensible defaults
        assert_eq!(config.http_proxy.port, 8095);
        assert_eq!(config.http_proxy.listen_addr, "0.0.0.0");
        // state_dir defaults to ./dataplane-state
        assert_eq!(config.state_dir, "./dataplane-state");
        assert_eq!(
            config.state_file_path(),
            PathBuf::from("./dataplane-state/state.json")
        );
    }

    #[test]
    fn test_state_dir_derives_state_file_path() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
state_dir: "/var/lib/indexify"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.state_dir, "/var/lib/indexify");
        assert_eq!(
            config.state_file_path(),
            PathBuf::from("/var/lib/indexify/state.json")
        );
    }

    #[test]
    fn test_local_env_config() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.env, "local");
        assert_eq!(config.http_proxy.port, 8095);
    }

    #[test]
    fn test_production_config() {
        let yaml = r#"
env: production
server_addr: "http://indexify.example.com:8901"
http_proxy:
  port: 8080
  advertise_address: "worker.example.com:8080"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.env, "production");
        assert_eq!(config.http_proxy.port, 8080);
        assert_eq!(
            config.http_proxy.get_advertise_address(),
            "worker.example.com:8080"
        );
    }

    #[test]
    fn test_http_proxy_socket_addr() {
        let config = HttpProxyConfig {
            port: 9000,
            listen_addr: "127.0.0.1".to_string(),
            advertise_address: None,
            upstream: UpstreamConfig::default(),
        };
        assert_eq!(config.socket_addr(), "127.0.0.1:9000");
    }

    #[test]
    fn test_parse_yaml_with_mtls() {
        let yaml = r#"
env: production
server_addr: "https://indexify.example.com:8901"
tls:
  enabled: true
  ca_cert_path: "/etc/certs/ca.pem"
  client_cert_path: "/etc/certs/client.pem"
  client_key_path: "/etc/certs/client-key.pem"
  domain_name: "indexify.example.com"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.env, "production");
        assert!(config.tls.enabled);
        assert_eq!(
            config.tls.ca_cert_path,
            Some("/etc/certs/ca.pem".to_string())
        );
    }

    #[test]
    fn test_invalid_mtls_config() {
        // mTLS requires both cert and key
        let yaml = r#"
env: local
server_addr: "https://indexify.example.com:8901"
tls:
  enabled: true
  client_cert_path: "/etc/certs/client.pem"
"#;
        let result = DataplaneConfig::from_yaml_str(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_server_addr_requires_scheme() {
        // server_addr without scheme should fail
        let yaml = r#"
env: local
server_addr: "indexify.example.com:8901"
"#;
        let result = DataplaneConfig::from_yaml_str(yaml);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must include a scheme")
        );
    }

    #[test]
    fn test_server_addr_with_http_scheme() {
        let yaml = r#"
env: local
server_addr: "http://indexify.example.com:8901"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.server_addr, "http://indexify.example.com:8901");
    }

    #[test]
    fn test_server_addr_with_https_scheme() {
        let yaml = r#"
env: local
server_addr: "https://indexify.example.com:8901"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(config.server_addr, "https://indexify.example.com:8901");
    }

    #[test]
    fn test_resource_overrides() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
resource_overrides:
  cpu_count: 2
  memory_bytes: 4294967296
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        let overrides = config.resource_overrides.unwrap();
        assert_eq!(overrides.cpu_count, Some(2));
        assert_eq!(overrides.memory_bytes, Some(4294967296));
        assert_eq!(overrides.disk_bytes, None);
    }

    #[test]
    fn test_docker_driver_with_runtime() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
driver:
  type: docker
  runtime: runsc
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        match &config.driver {
            DriverConfig::Docker {
                runtime, network, ..
            } => {
                assert_eq!(runtime.as_deref(), Some("runsc"));
                assert_eq!(network.as_deref(), None);
            }
            _ => panic!("Expected Docker driver"),
        }
    }

    #[test]
    fn test_docker_driver_with_network() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
driver:
  type: docker
  network: host
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        match &config.driver {
            DriverConfig::Docker {
                network, runtime, ..
            } => {
                assert_eq!(network.as_deref(), Some("host"));
                assert_eq!(runtime.as_deref(), None);
            }
            _ => panic!("Expected Docker driver"),
        }
    }

    #[test]
    fn test_docker_driver_with_all_options() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
driver:
  type: docker
  address: "unix:///var/run/docker.sock"
  runtime: runsc
  network: my-network
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        match &config.driver {
            DriverConfig::Docker {
                address,
                runtime,
                network,
                ..
            } => {
                assert_eq!(address.as_deref(), Some("unix:///var/run/docker.sock"));
                assert_eq!(runtime.as_deref(), Some("runsc"));
                assert_eq!(network.as_deref(), Some("my-network"));
            }
            _ => panic!("Expected Docker driver"),
        }
    }

    #[cfg(feature = "firecracker")]
    #[test]
    fn test_firecracker_driver_config() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
driver:
  type: firecracker
  kernel_image_path: "/opt/firecracker/vmlinux"
  base_rootfs_image: "/opt/firecracker/rootfs.ext4"
  cni_network_name: "indexify-fc"
  guest_gateway: "192.168.30.1"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        match &config.driver {
            DriverConfig::Firecracker {
                kernel_image_path,
                base_rootfs_image,
                cni_network_name,
                guest_gateway,
                firecracker_binary,
                default_rootfs_size_bytes,
                cni_bin_path,
                guest_netmask,
                default_vcpu_count,
                default_memory_mib,
                state_dir,
                log_dir,
            } => {
                assert_eq!(kernel_image_path, "/opt/firecracker/vmlinux");
                assert_eq!(base_rootfs_image, "/opt/firecracker/rootfs.ext4");
                assert_eq!(cni_network_name, "indexify-fc");
                assert_eq!(guest_gateway, "192.168.30.1");
                // Defaults
                assert!(firecracker_binary.is_none());
                assert!(default_rootfs_size_bytes.is_none());
                assert!(cni_bin_path.is_none());
                assert!(guest_netmask.is_none());
                assert!(default_vcpu_count.is_none());
                assert!(default_memory_mib.is_none());
                assert!(state_dir.is_none());
                assert!(log_dir.is_none());
            }
            _ => panic!("Expected Firecracker driver"),
        }
    }

    #[cfg(feature = "firecracker")]
    #[test]
    fn test_firecracker_driver_config_with_all_options() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
driver:
  type: firecracker
  firecracker_binary: "/usr/local/bin/firecracker"
  kernel_image_path: "/opt/firecracker/vmlinux"
  default_rootfs_size_bytes: 2147483648
  base_rootfs_image: "/opt/firecracker/rootfs.ext4"
  cni_network_name: "indexify-fc"
  cni_bin_path: "/usr/lib/cni"
  guest_gateway: "10.0.0.1"
  guest_netmask: "255.255.0.0"
  default_vcpu_count: 4
  default_memory_mib: 1024
  state_dir: "/var/lib/indexify/fc"
  log_dir: "/var/log/indexify/fc"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        match &config.driver {
            DriverConfig::Firecracker {
                firecracker_binary,
                default_rootfs_size_bytes,
                cni_bin_path,
                guest_netmask,
                default_vcpu_count,
                default_memory_mib,
                state_dir,
                log_dir,
                ..
            } => {
                assert_eq!(
                    firecracker_binary.as_deref(),
                    Some("/usr/local/bin/firecracker")
                );
                assert_eq!(*default_rootfs_size_bytes, Some(2147483648));
                assert_eq!(cni_bin_path.as_deref(), Some("/usr/lib/cni"));
                assert_eq!(guest_netmask.as_deref(), Some("255.255.0.0"));
                assert_eq!(*default_vcpu_count, Some(4));
                assert_eq!(*default_memory_mib, Some(1024));
                assert_eq!(state_dir.as_deref(), Some("/var/lib/indexify/fc"));
                assert_eq!(log_dir.as_deref(), Some("/var/log/indexify/fc"));
            }
            _ => panic!("Expected Firecracker driver"),
        }
    }

    #[cfg(feature = "firecracker")]
    #[test]
    fn test_firecracker_state_dir_derived_from_top_level() {
        // When driver doesn't set state_dir/log_dir, they derive from
        // the top-level state_dir.
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
state_dir: "/data/indexify"
driver:
  type: firecracker
  kernel_image_path: "/opt/firecracker/vmlinux"
  base_rootfs_image: "/opt/firecracker/rootfs.ext4"
  cni_network_name: "indexify-fc"
  guest_gateway: "192.168.30.1"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(
            config.firecracker_state_dir(),
            PathBuf::from("/data/indexify/firecracker")
        );
        assert_eq!(
            config.firecracker_log_dir(),
            PathBuf::from("/data/indexify/firecracker/logs")
        );
        assert_eq!(
            config.state_file_path(),
            PathBuf::from("/data/indexify/state.json")
        );
    }

    #[cfg(feature = "firecracker")]
    #[test]
    fn test_firecracker_state_dir_explicit_override() {
        // When driver explicitly sets state_dir/log_dir, those take
        // precedence over derivation from top-level state_dir.
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
state_dir: "/data/indexify"
driver:
  type: firecracker
  kernel_image_path: "/opt/firecracker/vmlinux"
  base_rootfs_image: "/opt/firecracker/rootfs.ext4"
  cni_network_name: "indexify-fc"
  guest_gateway: "192.168.30.1"
  state_dir: "/custom/fc-state"
  log_dir: "/custom/fc-logs"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert_eq!(
            config.firecracker_state_dir(),
            PathBuf::from("/custom/fc-state")
        );
        assert_eq!(
            config.firecracker_log_dir(),
            PathBuf::from("/custom/fc-logs")
        );
    }

    #[test]
    fn test_resource_overrides_default() {
        let yaml = r#"
env: local
server_addr: "http://localhost:8901"
"#;
        let config = DataplaneConfig::from_yaml_str(yaml).unwrap();
        assert!(config.resource_overrides.is_none());
    }
}
