use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use nanoid::nanoid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_bundle_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "generate_executor_id")]
    pub executor_id: String,
    pub server_http_addr: String,
    pub server_grpc_addr: String,
    pub listen_addr: String,
    #[serde(default)]
    pub function_uris: Vec<String>,
    pub cache_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring_server_addr: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub catalog_entry_name: Option<String>,
    pub tls: Option<TlsConfig>,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
}

fn generate_executor_id() -> String {
    format!("executor-{}", nanoid!(10))
}

fn default_heartbeat_interval() -> u64 {
    30
}

impl Default for Config {
    fn default() -> Self {
        Config {
            executor_id: generate_executor_id(),
            server_http_addr: "localhost:8900".to_string(),
            server_grpc_addr: "localhost:8901".to_string(),
            listen_addr: "localhost:8902".to_string(),
            function_uris: vec![],
            cache_dir: "./cache".to_string(),
            monitoring_server_addr: "localhost:7000".to_string().into(),
            catalog_entry_name: None,
            labels: vec![],
            tls: None,
            heartbeat_interval_secs: default_heartbeat_interval(),
        }
    }
}

impl Config {
    pub fn from_path(path: &str) -> anyhow::Result<Config> {
        let config: Config = Figment::new()
            .merge(Yaml::file(path))
            .extract()?;
        Ok(config)
    }
}