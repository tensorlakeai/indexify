use anyhow::Result;
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::fs;
use strum_macros::Display;

const OPENAI_DUMMY_KEY: &str = "xxxxx";

/// Enum representing the different kinds of text embedding models available for use.
/// Each variant is associated with specific dimensions, which represent the size of the embeddings.
#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum EmbeddingModelKind {
    #[serde(rename = "all-minilm-l12-v2")]
    #[strum(serialize = "all-minilm-l12-v2")]
    AllMiniLmL12V2,

    #[serde(rename = "all-minilm-l6-v2")]
    #[strum(serialize = "all-minilm-l6-v2")]
    AllMiniLmL6V2,

    #[serde(rename = "all-mpnet-base-v2")]
    #[strum(serialize = "all-mpnet-base-v2")]
    AllMpnetBaseV2,

    #[serde(rename = "all-distilroberta-v1")]
    #[strum(serialize = "all-distilroberta-v1")]
    AllDistilrobertaV1,

    /// T5 Model
    #[serde(rename = "t5-base")]
    #[strum(serialize = "t5-base")]
    T5Base,

    /// OpenAI Ada Model
    #[serde(rename = "text-embedding-ada-002")]
    #[strum(serialize = "text-embedding-ada-002")]
    OpenAIAda02,

    #[serde(rename = "dpr")]
    #[strum(serialize = "dpr")]
    DPRModel,
}

#[derive(Debug, Clone, Serialize, Deserialize, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum MemoryStoragePolicyKind {
    #[serde(rename = "indefinite")]
    Indefinite,
    #[serde(rename = "window")]
    Window,
    #[serde(rename = "lru")]
    Lru,
}

/// Enum representing the different kinds of devices on which the text embedding models can be run.
/// The available options are CPU, GPU, and Remote (for remote services such as OpenAI).
#[derive(Debug, Clone, Serialize, Deserialize, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum DeviceKind {
    #[serde(rename = "cpu")]
    Cpu,
    #[serde(rename = "gpu")]
    Gpu,
    #[serde(rename = "remote")]
    Remote,
}

/// Struct representing the configuration of a text embedding model.
/// It includes the kind of model being used (e.g., AllMiniLmL12V2) and the kind of device on which the model will run (e.g., CPU).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EmbeddingModel {
    #[serde(rename = "model")]
    pub model_kind: EmbeddingModelKind,
    #[serde(rename = "device")]
    pub device_kind: DeviceKind,
}

/// Struct representing the configuration of memory storage data structures.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MemoryStoragePolicy {
    #[serde(rename = "policy")]
    pub policy_kind: MemoryStoragePolicyKind,
    #[serde(rename = "size")]
    pub window_size: Option<usize>,
    #[serde(rename = "capacity")]
    pub capacity: Option<usize>,
}

/// Struct representing the configuration for OpenAI.
/// It includes the API key required for accessing OpenAI's services.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenAIConfig {
    pub api_key: String,
}

/// Enum representing the different kinds of index stores available for use.
/// The available options include Qdrant, which is a vector search engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum IndexStoreKind {
    Qdrant,
}

/// Struct representing the configuration for Qdrant, a vector search engine.
/// It includes the address of the Qdrant service.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct QdrantConfig {
    pub addr: String,
}

/// Struct representing the configuration for the vector index.
/// It includes the kind of index store being used (e.g., Qdrant) and any additional configuration specific to that index store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct VectorIndexConfig {
    pub index_store: IndexStoreKind,
    pub qdrant_config: Option<QdrantConfig>,
    pub db_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServerConfig {
    pub listen_addr: String,
    pub available_models: Vec<EmbeddingModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai: Option<OpenAIConfig>,
    pub index_config: Option<VectorIndexConfig>,
    pub memory_policies: Vec<MemoryStoragePolicy>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8900".to_string(),
            available_models: vec![
                EmbeddingModel {
                    model_kind: EmbeddingModelKind::AllMiniLmL12V2,
                    device_kind: DeviceKind::Cpu,
                },
                EmbeddingModel {
                    model_kind: EmbeddingModelKind::OpenAIAda02,
                    device_kind: DeviceKind::Remote,
                },
            ],
            openai: Some(OpenAIConfig {
                api_key: OPENAI_DUMMY_KEY.into(),
            }),
            index_config: None,
            memory_policies: vec![MemoryStoragePolicy {
                policy_kind: MemoryStoragePolicyKind::Indefinite,
                window_size: None,
                capacity: None,
            }],
        }
    }
}

impl ServerConfig {
    pub fn from_path(path: String) -> Result<Self> {
        let config_str: String = fs::read_to_string(path)?;
        let mut config: ServerConfig = Figment::new()
            .merge(Yaml::string(&config_str))
            .merge(Env::prefixed("INDEXIFY_"))
            .extract()?;

        // TODO Merge the openai api key from env only if there is nothing set in config
        // or it's not dummy
        if let Ok(openai_api_key) = std::env::var("OPENAI_API_KEY") {
            let openai_config = OpenAIConfig {
                api_key: openai_api_key,
            };
            config.openai = Some(openai_config);
        }
        Ok(config)
    }

    pub fn generate(path: String) -> Result<()> {
        let config = ServerConfig::default();
        let str = serde_yaml::to_string(&config)?;
        std::fs::write(path, str)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::server_config::OPENAI_DUMMY_KEY;

    #[test]
    fn pasrse_config() {
        // Uses the sample config file to test the config parsing
        let config = super::ServerConfig::from_path("sample_config.yaml".to_string()).unwrap();
        assert_eq!(3, config.available_models.len());
        assert_eq!(OPENAI_DUMMY_KEY, config.openai.unwrap().api_key);
        assert_eq!(
            config.index_config.clone().unwrap().index_store,
            super::IndexStoreKind::Qdrant
        );
        assert_eq!(
            config.index_config.unwrap().qdrant_config.unwrap().addr,
            "http://172.20.0.8:6334".to_string()
        );
        assert_eq!(
            "indefinite",
            config.memory_policies[0].policy_kind.to_string()
        );
    }
}
