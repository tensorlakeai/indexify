use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub socket_path: String,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                socket_path: "/tmp/gateway.sock".to_string(),
                log_level: "info".to_string(),
            },
        }
    }
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn merge_with_args(&mut self, socket_path: Option<String>, log_level: Option<String>) {
        if let Some(path) = socket_path {
            self.server.socket_path = path;
        }
        if let Some(level) = log_level {
            self.server.log_level = level;
        }
    }
}