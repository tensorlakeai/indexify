use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;

const LOCAL_ENV: &str = "local";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_bundle_path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContainerDriverType {
    #[default]
    Docker,
    ForkExec,
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkExecConfig {
    /// Base directory for function executor binaries
    #[serde_inline_default("/usr/local/bin/indexify".to_string())]
    pub bin_path: String,
    /// Working directory for spawned processes (uses tmpfs by default)
    #[serde_inline_default("/tmp/indexify".to_string())]
    pub work_dir: String,
}

impl Default for ForkExecConfig {
    fn default() -> Self {
        Self {
            bin_path: "/usr/local/bin/indexify".to_string(),
            work_dir: "/tmp/indexify".to_string(),
        }
    }
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde_inline_default(LOCAL_ENV.to_string())]
    pub env: String,
    #[serde_inline_default("0.0.0.0:8901".to_string())]
    pub server_grpc_addr: String,
    #[serde_inline_default("0.0.0.0:8900".to_string())]
    pub server_http_addr: String,
    #[serde(default)]
    pub tls_config: Option<TlsConfig>,
    #[serde(default)]
    pub container_driver: ContainerDriverType,
    #[serde(default)]
    pub fork_exec: ForkExecConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: LOCAL_ENV.to_string(),
            server_grpc_addr: "0.0.0.0:8901".to_string(),
            server_http_addr: "0.0.0.0:8900".to_string(),
            tls_config: None,
            container_driver: ContainerDriverType::default(),
            fork_exec: ForkExecConfig::default(),
        }
    }
}

impl Config {
    pub fn from_path(path: &str) -> Result<Config> {
        let config_str = std::fs::read_to_string(path)?;
        Self::from_yaml_str(&config_str)
    }

    fn from_yaml_str(config_str: &str) -> Result<Config> {
        let config: Config = serde_saphyr::from_str(config_str)?;
        Ok(config)
    }

    pub fn structured_logging(&self) -> bool {
        self.env != LOCAL_ENV
    }
}