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
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: LOCAL_ENV.to_string(),
            server_grpc_addr: "0.0.0.0:8901".to_string(),
            server_http_addr: "0.0.0.0:8900".to_string(),
            tls_config: None,
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