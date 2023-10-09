use anyhow::{anyhow, Result};
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use local_ip_address;
use serde::{Deserialize, Serialize};
use std::{
    fmt, fs,
    net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtractorDriver {
    #[serde(rename = "builtin")]
    BuiltIn,

    #[serde(rename = "python")]
    Python,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Extractor {
    pub name: String,
    pub path: String,
    pub driver: ExtractorDriver,
}

impl Default for Extractor {
    fn default() -> Self {
        Self {
            name: "default_embedder".to_string(),
            path: "MiniLML6Extractor".to_string(),
            driver: ExtractorDriver::Python,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum_macros::Display)]
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
    pub opensearch_basic: Option<OpenSearchBasicConfig>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            index_store: IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig::default()),
            pg_vector_config: Some(PgVectorConfig::default()),
            opensearch_basic: Some(OpenSearchBasicConfig::default()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ExtractorPackageConfig {
    pub name: String,
    pub module: String,
    pub gpu: bool,
    pub system_dependencies: Vec<String>,
    pub python_dependencies: Vec<String>,
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
    pub listen_addr: NetworkAddress,
    #[serde(default)]
    pub advertise_addr: NetworkAddress,
    #[serde(default = "default_executor_port")]
    pub listen_port: u64,
    pub executor_id: Option<String>,
    pub extractor: Extractor,
    pub blob_storage: BlobStorageConfig,
    pub index_config: VectorIndexConfig,
    pub db_url: String,
    pub coordinator_addr: NetworkAddress,
}

impl ExecutorConfig {
    pub fn from_path(path: &str) -> Result<ExecutorConfig, anyhow::Error> {
        let config_str: String = fs::read_to_string(path)?;
        let config: ExecutorConfig = Figment::new()
            .merge(Yaml::string(&config_str))
            .merge(Env::prefixed("INDEXIFY_"))
            .extract()?;

        Ok(config)
    }

    pub fn listen_addr_sock(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.listen_addr, self.listen_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!("Failed to parse listen address {} :{}", addr, e.to_string())
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServerConfig {
    #[serde(default)]
    pub listen_addr: NetworkAddress,
    #[serde(default = "default_server_port")]
    pub listen_port: u64,
    pub index_config: VectorIndexConfig,
    pub db_url: String,
    #[serde(default)]
    pub coordinator_addr: NetworkAddress,
    #[serde(default = "default_coordinator_port")]
    pub coordinator_port: u64,
    pub blob_storage: BlobStorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".into(),
            listen_port: default_server_port(),
            index_config: VectorIndexConfig::default(),
            db_url: "sqlite://indexify.db".into(),
            coordinator_addr: "0.0.0.0".into(),
            coordinator_port: default_coordinator_port(),
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
        let addr = format!("{}:{}", self.listen_addr, self.listen_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!("Failed to parse listen address {} :{}", addr, e.to_string())
        })
    }

    pub fn coordinator_addr_sock(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.coordinator_addr, self.coordinator_port);
        addr.parse().map_err(|e: AddrParseError| {
            anyhow!(
                "Failed to parse coordinator address{}: {}",
                addr,
                e.to_string()
            )
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
