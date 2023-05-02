use crate::{EmbeddingGenerator, EmbeddingGeneratorError};

use super::server_config::{self};
use anyhow::Result;
use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
use async_openai::{Client, Embeddings};
use async_trait::async_trait;

/// A struct that represents an interface to the OpenAI API for generating text embeddings.
///
/// This struct provides methods for generating text embeddings using the OpenAI API.
/// It supports generating embeddings for multiple input texts using a specified model.
pub struct OpenAI {
    client: Client,
    model: String,
}

impl OpenAI {
    /// Creates a new instance of `OpenAI` with the specified configuration.
    ///
    /// This method initializes the OpenAI client with the provided API key and sets the model
    /// to be used for generating embeddings.
    ///
    /// # Arguments
    ///
    /// * `openai_config` - The configuration for the OpenAI API, including the API key.
    /// * `model_config` - The configuration for the embedding model to be used.
    ///
    /// # Returns
    ///
    /// * A result containing an instance of `OpenAI` if successful, or an `EmbeddingGeneratorError`
    ///   if an error occurs.
    pub fn new(
        openai_config: server_config::OpenAIConfig,
        model_config: server_config::EmbeddingModel,
    ) -> Result<Self, EmbeddingGeneratorError> {
        let client = Client::new().with_api_key(openai_config.api_key);
        Ok(Self {
            client,
            model: model_config.model_kind.to_string(),
        })
    }
}

#[async_trait]
impl EmbeddingGenerator for OpenAI {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
        _model: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        let embeddings = Embeddings::new(&self.client);
        let response = embeddings
            .create(CreateEmbeddingRequest {
                input: EmbeddingInput::StringArray(inputs),
                model: self.model.clone(),
                user: None,
            })
            .await
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))?;
        let mut embeddings: Vec<Vec<f32>> = Vec::new();
        for embedding in response.data {
            embeddings.push(embedding.embedding);
        }
        Ok(embeddings)
    }

    fn dimensions(&self, model: String) -> Result<u64, EmbeddingGeneratorError> {
        match model.as_str() {
            "text-embedding-ada-002" => Ok(1536),
            _ => Err(EmbeddingGeneratorError::ModelNotFound(model)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::OpenAIConfig;

    use super::*;

    #[tokio::test]
    async fn test_embeddings() {
        let api_key = std::env::var("OPENAI_API_KEY");
        if let Err(_err) = api_key {
            return;
        }
        let openai = OpenAI::new(
            OpenAIConfig {
                api_key: api_key.unwrap(),
            },
            crate::EmbeddingModel {
                model_kind: crate::EmbeddingModelKind::OpenAIAda02,
                device_kind: crate::DeviceKind::Remote,
            },
        )
        .unwrap();
        let embeddings = openai
            .generate_embeddings(vec!["hello".into(), "world".into()], "".into())
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 1536);
    }
}
