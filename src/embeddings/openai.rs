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

    tokenizer: Result<tiktoken_rs::CoreBPE>,
}

impl OpenAI {
    /// Creates a new instance of `OpenAI` with the specified configuration.
    ///
    /// This method initializes the OpenAI client with the provided API key and sets the model
    /// to be used for generating embeddings.
    pub fn new(
        openai_config: server_config::OpenAIConfig,
        model_config: server_config::EmbeddingModel,
    ) -> Result<Self, EmbeddingGeneratorError> {
        let client = Client::new().with_api_key(openai_config.api_key);
        Ok(Self {
            client,
            model: model_config.model_kind.to_string(),
            tokenizer: tiktoken_rs::cl100k_base(),
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

    async fn tokenize_text(
        &self,
        inputs: Vec<String>,
        _model: String,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError> {
        let mut results = Vec::new();
        for input in &inputs {
            let tokens = self
                .tokenizer
                .as_ref()
                .unwrap()
                .split_by_token(input, true)
                .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))?;
            results.push(tokens);
        }
        Ok(results)
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
        _model: String,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError> {
        let mut result: Vec<Vec<i64>> = Vec::new();
        for input in inputs {
            let tokens: Vec<i64> = self
                .tokenizer
                .as_ref()
                .unwrap()
                .encode_ordinary(input.as_str())
                .into_iter()
                .map(|x| x as i64)
                .collect();
            result.push(tokens);
        }
        Ok(result)
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<i64>>,
        _model: String,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        let mut result: Vec<String> = Vec::new();
        for input in inputs {
            let input_usize = input.into_iter().map(|x| x as usize).collect();
            let text: String = self
                .tokenizer
                .as_ref()
                .unwrap()
                .decode(input_usize)
                .map_err(|e| EmbeddingGeneratorError::InternalError(e.to_string()))?;
            result.push(text);
        }
        Ok(result)
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

    #[tokio::test]
    async fn test_tokenization() {
        let openai = OpenAI::new(
            OpenAIConfig {
                api_key: "not needed".into(),
            },
            crate::EmbeddingModel {
                model_kind: crate::EmbeddingModelKind::OpenAIAda02,
                device_kind: crate::DeviceKind::Remote,
            },
        )
        .unwrap();
        let inputs = vec!["hello world so long that it doesn't fit my mind ".repeat(10)];
        let tokenized_texts = openai.tokenize_text(inputs, "".into()).await.unwrap();
        assert_eq!(tokenized_texts.len(), 1);
        assert_eq!(tokenized_texts[0].len(), 111);
    }

    #[tokio::test]
    async fn test_tokenization_encode_decode() {
        let openai = OpenAI::new(
            OpenAIConfig {
                api_key: "not needed".into(),
            },
            crate::EmbeddingModel {
                model_kind: crate::EmbeddingModelKind::OpenAIAda02,
                device_kind: crate::DeviceKind::Remote,
            },
        )
        .unwrap();
        let inputs = vec!["embiid is the mvp".into()];
        let tokens = openai
            .tokenize_encode(inputs.clone(), "".into())
            .await
            .unwrap();

        let tokenized_text = openai.tokenize_decode(tokens, "".into()).await.unwrap();
        assert_eq!(inputs, tokenized_text);
    }
}
