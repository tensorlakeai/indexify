use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;

use anyhow::Result;
use thiserror::Error;

use rust_bert::pipelines::sentence_embeddings::{
    SentenceEmbeddingsBuilder, SentenceEmbeddingsModel, SentenceEmbeddingsModelType,
};

use crate::server_config;

#[derive(Error, Debug)]
pub enum EmbeddingGeneratorError {
    #[error("model `{0}` not found")]
    ModelNotFound(String),

    #[error("model inference error: `{0}`")]
    ModelError(String),

    #[error("model loading error: `{0}`")]
    ModelLoadingError(String),

    #[error("internal error: `{0}`")]
    InternalError(String),
}

type Message = (
    String,
    Vec<String>,
    oneshot::Sender<Result<Vec<Vec<f32>>, EmbeddingGeneratorError>>,
);
pub struct EmbeddingGenerator {
    sender: mpsc::SyncSender<Message>,
}

impl EmbeddingGenerator {
    pub fn new(
        models_to_load: Vec<server_config::SentenceEmbeddingModels>,
    ) -> Result<EmbeddingGenerator, EmbeddingGeneratorError> {
        let (sender, receiver) = mpsc::sync_channel(100);
        thread::spawn(move || {
            if let Err(err) = Self::runner(receiver, models_to_load) {
                tracing::error!("embedding generator runner exited with error: {}", err);
            }
        });
        Ok(EmbeddingGenerator { sender })
    }

    fn runner(
        receiver: mpsc::Receiver<Message>,
        models_to_load: Vec<server_config::SentenceEmbeddingModels>,
    ) -> Result<(), EmbeddingGeneratorError> {
        let mut models: HashMap<String, SentenceEmbeddingsModel> = HashMap::new();
        for model_name in &models_to_load {
            match model_name {
                server_config::SentenceEmbeddingModels::AllMiniLmL12V2 => {
                    let model = SentenceEmbeddingsBuilder::remote(
                        SentenceEmbeddingsModelType::AllMiniLmL12V2,
                    )
                    .create_model()
                    .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
                    models.insert("all-minilm-l12-v2".into(), model);
                }
            }
        }
        for (model_name, inputs, sender) in receiver.iter() {
            let model = models.get(&model_name);
            if model.is_none() {
                let _ = sender.send(Err(EmbeddingGeneratorError::ModelNotFound(model_name)));
                continue;
            }
            let result = model
                .unwrap()
                .encode(&inputs)
                .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()));
            let _ = sender.send(result);
        }
        Ok(())
    }

    pub async fn generate_embeddings(
        &self,
        texts: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        let (tx, rx) = oneshot::channel();
        let _ = self.sender.send((model, texts, tx));
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(EmbeddingGeneratorError::InternalError(
                "channel closed unexpectedly".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_generate_embeddings_all_mini_lm_l12v2() {
        use super::*;
        let inputs = vec![
            "Hello, world!".to_string(),
            "Hello, NBA!".to_string(),
            "Hello, NFL!".to_string(),
        ];
        let embedding_generator =
            EmbeddingGenerator::new(vec![server_config::SentenceEmbeddingModels::AllMiniLmL12V2])
                .unwrap();
        let embeddings = embedding_generator
            .generate_embeddings(inputs, "all-minilm-l12-v2".into())
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        assert_eq!(embeddings[0].len(), 384);
    }
}
