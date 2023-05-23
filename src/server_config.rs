use anyhow::Result;
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::fs;

const OPENAI_DUMMY_KEY: &str = "xxxxx";

/// Enum representing the different kinds of text embedding models available for use.
/// Each variant is associated with specific dimensions, which represent the size of the embeddings.
#[derive(
    Debug, Clone, Serialize, Deserialize, strum_macros::Display, strum_macros::EnumProperty,
)]
#[strum(serialize_all = "kebab-case")]
pub enum EmbeddingModelKind {
    #[strum(props(dimensions = "384"))]
    #[strum(serialize = "all-minilm-l12-v2")]
    #[serde(rename = "all-minilm-l12-v2")]
    AllMiniLmL12V2,

    #[strum(props(dimensions = "384"))]
    #[strum(serialize = "all-minilm-l6-v2")]
    #[serde(rename = "all-minilm-l6-v2")]
    AllMiniLmL6V2,

    #[strum(props(dimensions = "768"))]
    #[strum(serialize = "all-mpnet-base-v2")]
    #[serde(rename = "all-mpnet-base-v2")]
    AllMpnetBaseV2,

    #[strum(props(dimensions = "768"))]
    #[strum(serialize = "all-distilroberta-v1")]
    #[serde(rename = "all-distilroberta-v1")]
    AllDistilrobertaV1,

    /// T5 Model
    #[strum(props(dimensions = "768"))]
    #[strum(serialize = "t5-base")]
    #[serde(rename = "t5-base")]
    T5Base,

    /// OpenAI Ada Model
    #[strum(props(dimensions = "1536"))]
    #[strum(serialize = "text-embedding-ada-002")]
    #[serde(rename = "text-embedding-ada-002")]
    OpenAIAda02,
}

#[derive(Debug, Clone, Serialize, Deserialize, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum MemoryPolicyKind {
    #[serde(rename = "simple")]
    Simple,
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

/// Struct representing the configuration of a conversation history data structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MemoryPolicy {
    #[serde(rename = "policy")]
    pub policy_kind: MemoryPolicyKind,
    #[serde(rename = "size")]
    pub window_size: Option<usize>,
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

/// Struct representing the server configuration.
/// It includes the address on which the server will listen, the available text embedding models, the OpenAI configuration (if applicable), and the vector index configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ServerConfig {
    pub listen_addr: String,
    pub available_models: Vec<EmbeddingModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openai: Option<OpenAIConfig>,
    pub index_config: Option<VectorIndexConfig>,
    pub memory_policies: Vec<MemoryPolicy>,
}

impl Default for ServerConfig {
    /// Provides default values for the server configuration.
    /// The default configuration includes a default listening address, a set of available models, and an OpenAI configuration with a dummy API key.
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
            memory_policies: vec![MemoryPolicy {
                policy_kind: MemoryPolicyKind::Simple,
                window_size: None,
            }],
        }
    }
}

impl ServerConfig {
    /// Loads the server configuration from a file specified by the given path.
    /// The configuration file should be in YAML format and should contain the necessary configuration options.
    /// The configuration file can also be overridden by environment variables prefixed with `INDEXIFY_`.
    /// Note that a configuration file must exist, otherwise an error will be returned (even if the environment variables are set)
    ///
    /// # Arguments
    ///
    /// * path - The path to the configuration file.
    ///
    /// # Returns
    ///
    /// * A result containing the server configuration if successful, or an error if loading fails.
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

    /// Generates a default server configuration and writes it to a file specified by the given path.
    /// The default configuration includes a default listening address, a set of available models, and an OpenAI configuration with a dummy API key.
    /// This method is useful for generating an initial configuration file that can be manually edited later.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file where the default configuration will be written.
    ///
    /// # Returns
    ///
    /// * A result indicating success or failure of the operation.
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
        assert_eq!(2, config.available_models.len());
        assert_eq!(OPENAI_DUMMY_KEY, config.openai.unwrap().api_key);
        assert_eq!(
            config.index_config.clone().unwrap().index_store,
            super::IndexStoreKind::Qdrant
        );
        assert_eq!(
            config.index_config.unwrap().qdrant_config.unwrap().addr,
            "http://172.20.0.8:6334".to_string()
        );
        assert_eq!("simple", config.memory_policies[0].policy_kind.to_string());
    }
}
