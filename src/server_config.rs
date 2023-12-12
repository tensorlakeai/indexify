use std::{
    fmt,
    fs,
    net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
};

use anyhow::{anyhow, Error, Result};
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

fn default_executor_port() -> u64 {
    0
}

fn default_server_port() -> u64 {
    8900
}

fn default_coordinator_port() -> u64 {
    8950
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStorageConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    pub backend: String,
    pub s3: Option<S3Config>,
    pub disk: Option<DiskStorageConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum IndexStoreKind {
    Qdrant,
    PgVector,
    OpenSearchKnn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QdrantConfig {
    pub addr: String,
}

impl Default for QdrantConfig {
    fn default() -> Self {
        Self {
            addr: "http://127.0.0.1:6334".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenSearchBasicConfig {
    pub addr: String,
    pub username: String,
    pub password: String,
}

impl Default for OpenSearchBasicConfig {
    fn default() -> Self {
        Self {
            addr: "https://localhost:9200".into(),
            username: "admin".into(),
            password: "admin".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PgVectorConfig {
    pub addr: String,
    pub m: i32,
    pub efconstruction: i32,
    pub efsearch: i32,
}

impl Default for PgVectorConfig {
    fn default() -> Self {
        Self {
            addr: "postgres://postgres:postgres@localhost/indexify".into(),
            // Figure out best default parameters for the following values
            m: 16,
            efsearch: 64,
            efconstruction: 40,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct VectorIndexConfig {
    pub index_store: IndexStoreKind,
    pub qdrant_config: Option<QdrantConfig>,
    pub pg_vector_config: Option<PgVectorConfig>,
    pub open_search_basic: Option<OpenSearchBasicConfig>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            index_store: IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig::default()),
            pg_vector_config: Some(PgVectorConfig::default()),
            open_search_basic: Some(OpenSearchBasicConfig::default()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExtractorConfig {
    pub name: String,
    pub version: String,
    pub description: String,
    pub module: String,
    pub gpu: bool,
    pub system_dependencies: Vec<String>,
    pub python_dependencies: Vec<String>,
}

impl ExtractorConfig {
    pub fn from_path(path: &str) -> Result<ExtractorConfig> {
        let config = std::fs::read_to_string(path)?;
        let config: ExtractorConfig = serde_yaml::from_str(&config)?;
        Ok(config)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NetworkAddress(String);

impl Default for NetworkAddress {
    fn default() -> Self {
        // NOTE - This is not a perfect algorithm to probe the network address
        // we should advertise. The user should be setting this in the config.
        // 1. Probe for all the interfaces
        // 2. Find the one that is not loopback
        // 3. If there is nothing else - use loopback
        // 4. Return "" if there is no interfaces at all, and it will eventually fail.
        let ip = local_ip_address::local_ip().unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        Self(ip.to_string())
    }
}

impl fmt::Display for NetworkAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.clone())
    }
}

impl fmt::Debug for NetworkAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.clone())
    }
}

impl From<NetworkAddress> for String {
    fn from(addr: NetworkAddress) -> Self {
        addr.0
    }
}

impl From<&str> for NetworkAddress {
    fn from(addr: &str) -> Self {
        Self(addr.to_owned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExecutorConfig {
    #[serde(default)]
    pub listen_if: NetworkAddress,
    #[serde(default)]
    pub advertise_if: NetworkAddress,
    #[serde(default = "default_executor_port")]
    pub listen_port: u64,
    #[serde(default)]
    pub coordinator_addr: String,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            listen_if: NetworkAddress::default(),
            advertise_if: NetworkAddress::default(),
            listen_port: default_executor_port(),
            coordinator_addr: format!("localhost:{}", default_coordinator_port()),
        }
    }
}

impl ExecutorConfig {
    pub fn listen_addr_sock(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.listen_if, self.listen_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!("Failed to parse listen address {} :{}", addr, e.to_string())
        })
    }

    #[allow(dead_code)]
    pub fn with_advertise_ip(mut self, ip: Option<String>) -> Self {
        if let Some(ip) = ip {
            self.advertise_if = NetworkAddress(ip);
        }
        self
    }

    #[allow(dead_code)]
    pub fn with_advertise_port(mut self, port: Option<u64>) -> Self {
        if let Some(port) = port {
            self.listen_port = port;
        }
        self
    }

    pub fn with_advertise_addr(mut self, addr: Option<String>) -> Result<Self, Error> {
        if let Some(addr) = addr {
            let sock_addr: SocketAddr = addr.parse()?;
            self.advertise_if = NetworkAddress(sock_addr.ip().to_string());
            self.listen_port = sock_addr.port() as u64;
        }
        Ok(self)
    }

    pub fn with_coordinator_addr(mut self, addr: String) -> Self {
        self.coordinator_addr = addr;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServerConfig {
    #[serde(default)]
    pub listen_if: NetworkAddress,
    #[serde(default = "default_server_port")]
    pub listen_port: u64,
    #[serde(default = "default_coordinator_port")]
    pub coordinator_port: u64,
    pub index_config: VectorIndexConfig,
    pub db_url: String,
    #[serde(default)]
    pub coordinator_addr: String,
    pub blob_storage: BlobStorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_if: "0.0.0.0".into(),
            listen_port: default_server_port(),
            coordinator_port: default_coordinator_port(),
            index_config: VectorIndexConfig::default(),
            db_url: "postgres://postgres:postgres@localhost/indexify".into(),
            coordinator_addr: format!("localhost:{}", default_coordinator_port()),
            blob_storage: BlobStorageConfig {
                backend: "disk".to_string(),
                s3: None,
                disk: Some(DiskStorageConfig {
                    path: "blobs".to_string(),
                }),
            },
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: &str) -> Result<Self> {
        let config_str: String = fs::read_to_string(path)?;
        let config: ServerConfig = Figment::new()
            .merge(Yaml::string(&config_str))
            .merge(Env::prefixed("INDEXIFY_"))
            .extract()?;

        Ok(config)
    }

    pub fn generate(path: String) -> Result<()> {
        let config = ServerConfig::default();
        let str = serde_yaml::to_string(&config)?;
        std::fs::write(path, str)?;
        Ok(())
    }

    pub fn listen_addr_sock(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.listen_if, self.listen_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!("Failed to parse listen address {} :{}", addr, e.to_string())
        })
    }

    pub fn coordinator_lis_addr_sock(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.listen_if, self.coordinator_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!("Failed to parse listen address {} :{}", addr, e.to_string())
        })
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_config() {
        // Uses the sample config file to test the config parsing
        let config = super::ServerConfig::from_path("sample_config.yaml").unwrap();
        assert_eq!(
            config.index_config.index_store,
            super::IndexStoreKind::Qdrant
        );
        assert_eq!(
            config.index_config.qdrant_config.unwrap().addr,
            "http://qdrant:6334".to_string()
        );
    }
}
