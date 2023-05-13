use std::sync::mpsc;
use std::thread;

use async_trait::async_trait;

use rust_bert::pipelines::sentence_embeddings::{
    SentenceEmbeddingsBuilder, SentenceEmbeddingsModel, SentenceEmbeddingsModelType,
};
use rust_tokenizers::tokenizer::TruncationStrategy;

use super::server_config::{self, EmbeddingModelKind};
use super::{EmbeddingGenerator, EmbeddingGeneratorError};
use std::collections::HashMap;

enum ModelOperation {
    EncodeEmbeddings,
    Tokenize,
    TokenizeEncode,
    TokenizeDecode,
}

enum ModelResult {
    Embeddings(Vec<Vec<f32>>),
    Tokenized(Vec<Vec<String>>),
    TokenizedDecoded(Vec<String>),
    TokenizedEncoded(Vec<Vec<i64>>),
}

type Message = (
    String,
    Vec<String>,
    Vec<Vec<i64>>,
    ModelOperation,
    oneshot::Sender<Result<ModelResult, EmbeddingGeneratorError>>,
);

/// A struct that represents a collection of sentence transformer models.
///
/// This struct provides methods for generating sentence embeddings using
/// pre-trained transformer models. It supports multiple models and manages
/// them in a separate thread.
pub struct SentenceTransformerModels {
    sender: mpsc::SyncSender<Message>,
}

impl SentenceTransformerModels {
    /// Creates a new instance of `SentenceTransformerModels` and loads the specified models.
    ///
    /// This method spawns a new thread that runs the `runner` method, which is responsible
    /// for loading the specified models and processing incoming requests for generating
    /// embeddings.
    ///
    /// # Arguments
    ///
    /// * `models_to_load` - A vector of `EmbeddingModel` configurations specifying the models
    ///   to be loaded.
    ///
    /// # Returns
    ///
    /// * A result containing an `Arc` reference to the `SentenceTransformerModels` instance
    ///   if successful, or an `EmbeddingGeneratorError` if an error occurs.
    pub fn new(
        models_to_load: Vec<server_config::EmbeddingModel>,
    ) -> Result<Self, EmbeddingGeneratorError> {
        let (sender, receiver) = mpsc::sync_channel(100);
        thread::spawn(move || {
            if let Err(err) = Self::runner(receiver, models_to_load) {
                tracing::error!("embedding generator runner exited with error: {}", err);
            }
        });
        Ok(SentenceTransformerModels { sender })
    }

    /// This method is run in a separate thread and listens for incoming requests to generate
    /// embeddings using the loaded models. It sends the generated embeddings back to the caller
    /// through a one-shot channel.
    fn runner(
        receiver: mpsc::Receiver<Message>,
        models_to_load: Vec<server_config::EmbeddingModel>,
    ) -> Result<(), EmbeddingGeneratorError> {
        let mut models: HashMap<String, SentenceEmbeddingsModel> = HashMap::new();
        for model in &models_to_load {
            match &model.model_kind {
                EmbeddingModelKind::AllMiniLmL12V2 => {
                    let model = SentenceEmbeddingsBuilder::remote(
                        SentenceEmbeddingsModelType::AllMiniLmL12V2,
                    )
                    .create_model()
                    .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
                    models.insert(EmbeddingModelKind::AllMiniLmL12V2.to_string(), model);
                }
                EmbeddingModelKind::AllMiniLmL6V2 => {
                    let model = SentenceEmbeddingsBuilder::remote(
                        SentenceEmbeddingsModelType::AllMiniLmL6V2,
                    )
                    .create_model()
                    .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
                    models.insert(EmbeddingModelKind::AllMiniLmL6V2.to_string(), model);
                }
                EmbeddingModelKind::T5Base => {
                    let model = SentenceEmbeddingsBuilder::remote(
                        SentenceEmbeddingsModelType::SentenceT5Base,
                    )
                    .create_model()
                    .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
                    models.insert(EmbeddingModelKind::T5Base.to_string(), model);
                }
                _ => {
                    return Err(EmbeddingGeneratorError::InternalError(
                        "unknown model kind".into(),
                    ));
                }
            }
        }
        for (model_name, inputs, batched_tokens, model_operation, sender) in receiver.iter() {
            let model = models.get(&model_name);
            if model.is_none() {
                let _ = sender.send(Err(EmbeddingGeneratorError::ModelNotFound(model_name)));
                continue;
            }
            match model_operation {
                ModelOperation::EncodeEmbeddings => {
                    let result = model
                        .unwrap()
                        .encode(&inputs)
                        .map(ModelResult::Embeddings)
                        .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()));
                    let _ = sender.send(result);
                }
                ModelOperation::Tokenize => {
                    let tokenizer = model.unwrap().get_tokenizer();
                    let mut tokenized_inputs = Vec::new();
                    for input in &inputs {
                        tokenized_inputs.push(tokenizer.tokenize(input));
                    }
                    let _ = sender.send(Ok(ModelResult::Tokenized(tokenized_inputs)));
                }
                ModelOperation::TokenizeEncode => {
                    let tokenizer = model.unwrap().get_tokenizer();
                    let result =
                        tokenizer.encode_list(&inputs, 512, &TruncationStrategy::DoNotTruncate, 0);
                    let tokens: Vec<Vec<i64>> = result.into_iter().map(|t| t.token_ids).collect();
                    let _ = sender.send(Ok(ModelResult::TokenizedEncoded(tokens)));
                }
                ModelOperation::TokenizeDecode => {
                    let tokenizer = model.unwrap().get_tokenizer();
                    let mut results: Vec<String> = Vec::new();
                    for tokens in batched_tokens {
                        let result = tokenizer.decode(&tokens, true, true);
                        results.push(result);
                    }
                    let _ = sender.send(Ok(ModelResult::TokenizedDecoded(results)));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EmbeddingGenerator for SentenceTransformerModels {
    async fn generate_embeddings(
        &self,
        texts: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send((model, texts, vec![], ModelOperation::EncodeEmbeddings, tx));
        match rx.await {
            Ok(result) => match result {
                Ok(ModelResult::Embeddings(embeddings)) => Ok(embeddings),
                Err(err) => Err(err),
                Ok(_) => Err(EmbeddingGeneratorError::InternalError(
                    "unexpected tokenized result".into(),
                )),
            },
            Err(_) => Err(EmbeddingGeneratorError::InternalError(
                "channel closed unexpectedly".into(),
            )),
        }
    }

    fn dimensions(&self, model: String) -> Result<u64, EmbeddingGeneratorError> {
        match model.as_str() {
            "all-minilm-l12-v2" => Ok(384),
            "all-minilm-l6-v2" => Ok(384),
            "all-mpnet-base-v2" => Ok(768),
            "all-distilroberta-v1" => Ok(768),
            "t5-base" => Ok(768),
            _ => Err(EmbeddingGeneratorError::ModelNotFound(model)),
        }
    }

    async fn tokenize_text(
        &self,
        inputs: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<String>>, EmbeddingGeneratorError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .sender
            .send((model, inputs, vec![], ModelOperation::Tokenize, tx));

        match rx.await {
            Ok(result) => match result {
                Ok(ModelResult::Tokenized(tokenized_texts)) => Ok(tokenized_texts),
                Err(err) => Err(err),
                Ok(_) => Err(EmbeddingGeneratorError::InternalError(
                    "unexpected embeddings result".into(),
                )),
            },
            Err(_) => Err(EmbeddingGeneratorError::InternalError(
                "channel closed unexpectedly".into(),
            )),
        }
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<i64>>, EmbeddingGeneratorError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .sender
            .send((model, inputs, vec![], ModelOperation::TokenizeEncode, tx));

        match rx.await {
            Ok(result) => match result {
                Ok(ModelResult::TokenizedEncoded(tokens)) => Ok(tokens),
                Err(err) => Err(err),
                Ok(_) => Err(EmbeddingGeneratorError::InternalError(
                    "unexpected embeddings result".into(),
                )),
            },
            Err(_) => Err(EmbeddingGeneratorError::InternalError(
                "channel closed unexpectedly".into(),
            )),
        }
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<i64>>,
        model: String,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        let (tx, rx) = oneshot::channel();

        let _ = self
            .sender
            .send((model, vec![], inputs, ModelOperation::TokenizeDecode, tx));

        match rx.await {
            Ok(result) => match result {
                Ok(ModelResult::TokenizedDecoded(texts)) => Ok(texts),
                Err(err) => Err(err),
                Ok(_) => Err(EmbeddingGeneratorError::InternalError(
                    "unexpected embeddings result".into(),
                )),
            },
            Err(_) => Err(EmbeddingGeneratorError::InternalError(
                "channel closed unexpectedly".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use server_config::DeviceKind;
    use server_config::EmbeddingModelKind::AllMiniLmL12V2;

    #[tokio::test]
    async fn test_generate_embeddings_all_mini_lm_l12v2() {
        let inputs = vec![
            "Hello, world!".into(),
            "Hello, NBA!".into(),
            "Hello, NFL!".into(),
        ];
        let embedding_generator =
            SentenceTransformerModels::new(vec![server_config::EmbeddingModel {
                model_kind: AllMiniLmL12V2,
                device_kind: DeviceKind::Cpu,
            }])
            .unwrap();
        let embeddings = embedding_generator
            .generate_embeddings(inputs, "all-minilm-l12-v2".into())
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        assert_eq!(embeddings[0].len(), 384);
    }

    #[tokio::test]
    async fn test_tokenization() {
        let inputs = vec!["hello world so long that it doesn't fit my mind ".repeat(10)];
        let embedding_generator =
            SentenceTransformerModels::new(vec![server_config::EmbeddingModel {
                model_kind: AllMiniLmL12V2,
                device_kind: DeviceKind::Cpu,
            }])
            .unwrap();
        let tokenized_text = embedding_generator
            .tokenize_text(inputs, "all-minilm-l12-v2".into())
            .await
            .unwrap();
        assert_eq!(tokenized_text.len(), 1);
        assert_eq!(tokenized_text[0].len(), 120);
    }

    #[tokio::test]
    async fn test_tokenization_encode_decode() {
        let model: String = "all-minilm-l12-v2".into();
        let inputs = vec!["embiid is the mvp".into()];
        let embedding_generator =
            SentenceTransformerModels::new(vec![server_config::EmbeddingModel {
                model_kind: AllMiniLmL12V2,
                device_kind: DeviceKind::Cpu,
            }])
            .unwrap();
        let tokens = embedding_generator
            .tokenize_encode(inputs.clone(), model.clone())
            .await
            .unwrap();

        let tokenized_text = embedding_generator
            .tokenize_decode(tokens, model.clone())
            .await
            .unwrap();
        assert_eq!(inputs, tokenized_text);
    }
}
