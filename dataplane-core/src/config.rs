use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
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
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server_http_addr: "localhost:8900".to_string(),
            server_grpc_addr: "localhost:8901".to_string(),
            listen_addr: "localhost:8902".to_string(),
            function_uris: vec![],
            cache_dir: "./cache".to_string(),
            monitoring_server_addr: "localhost:7000".to_string().into(),
            labels: vec![],
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