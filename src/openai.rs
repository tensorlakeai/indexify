use super::server_config;
use anyhow::Result;
use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};
use async_openai::{Client, Embeddings};

pub struct OpenAI {
    client: Client,
    model: String,
}

impl OpenAI {
    pub fn new(
        openai_config: server_config::OpenAIConfig,
        model_config: server_config::EmbeddingModel,
    ) -> Result<Self> {
        let client = Client::new().with_api_key(openai_config.api_key);
        Ok(Self {
            client,
            model: model_config.model_kind.to_string(),
        })
    }

    pub async fn generate_embeddings(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>> {
        let embeddings = Embeddings::new(&self.client);
        let response = embeddings
            .create(CreateEmbeddingRequest {
                input: EmbeddingInput::StringArray(inputs),
                model: self.model.clone(),
                user: None,
            })
            .await?;
        let mut embeddings: Vec<Vec<f32>> = Vec::new();
        for embedding in response.data {
            embeddings.push(embedding.embedding);
        }
        Ok(embeddings)
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
            .generate_embeddings(vec!["hello".into(), "world".into()])
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 1536);
    }
}
