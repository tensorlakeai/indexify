use anyhow::Result;
use batched_fn::batched_fn;
use rust_bert::RustBertError;
use rust_bert::pipelines::sentence_embeddings::SentenceEmbeddingsBuilder;
use rust_bert::pipelines::sentence_embeddings::SentenceEmbeddingsModel;
use rust_bert::pipelines::sentence_embeddings::SentenceEmbeddingsModelType;

use crate::server_config;

pub struct EmbeddingGenerator {
    models_to_load: Vec<server_config::SentenceEmbeddingModels>,
}

impl EmbeddingGenerator {
    pub fn new(models_to_load: Vec<server_config::SentenceEmbeddingModels>) -> Result<Self> {
        Ok(Self { models_to_load })
    }

    pub async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
        _model: String,
    ) -> Result<Vec<Vec<f32>>> {
        let batched_generate = batched_fn! {
            handler = |batch: Vec<Vec<String>>, model: &SentenceEmbeddingsModel| -> Vec<Result<Vec<Vec<f32>>, RustBertError>> {
                let mut batched_result = Vec::with_capacity(batch.len());
                for input in batch {
                    let result = model.encode(&input);
                    batched_result.push(result);
                }
                batched_result
            };
            config = {
                max_batch_size: 1,
                max_delay: 100,
                channel_cap: Some(20),
            };
            context = {
                model: SentenceEmbeddingsBuilder::remote(SentenceEmbeddingsModelType::AllMiniLmL12V2).create_model().unwrap(),
            };
        };
        let batched_result = batched_generate(inputs).await;
        if let Ok(result) = batched_result {
            if let Ok(embeddings) = result {
                return Ok(embeddings);
            } else {
                return Err(anyhow::anyhow!("Error generating embeddings"));
            }
        }
        return Err(anyhow::anyhow!("error invoking model"))
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_generate_embeddings_all_mini_lm_l12v2() {
        use super::*;
        let inputs = vec![
            "Hello, world!".to_string(),
            "Hello, world!".to_string(),
            "Hello, world!".to_string(),
        ];
        let embedding_generator =
            EmbeddingGenerator::new(vec![server_config::SentenceEmbeddingModels::AllMiniLmL12V2])
                .unwrap();
        let embeddings = embedding_generator
            .generate_embeddings(inputs, "all-mini-lm-12v2".into())
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        assert_eq!(embeddings[0].len(), 384);
    }
}
