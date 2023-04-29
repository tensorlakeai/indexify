mod openai;
mod sentence_transformers;

use super::server_config::{
    self, EmbeddingModelKind::AllMiniLmL12V2, EmbeddingModelKind::AllMiniLmL6V2,
    EmbeddingModelKind::OpenAIAda02, EmbeddingModelKind::T5Base,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

use openai::OpenAI;
use sentence_transformers::SentenceTransformerModels;

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

    #[error("configuration `{0}`, missing for model `{1}`")]
    ConfigurationError(String, String),
}

pub type EmbeddingGeneratorTS = Arc<dyn EmbeddingGenerator + Sync + Send>;

#[async_trait]
pub trait EmbeddingGenerator {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError>;

    fn dimensions(&self, model: String) -> Result<u64, EmbeddingGeneratorError>;
}

pub struct EmbeddingRouter {
    router: HashMap<String, EmbeddingGeneratorTS>,

    model_names: Vec<String>,
}

impl EmbeddingRouter {
    pub fn new(config: Arc<server_config::ServerConfig>) -> Result<Self, EmbeddingGeneratorError> {
        let mut router: HashMap<String, EmbeddingGeneratorTS> = HashMap::new();
        let mut sentence_transformers: Vec<server_config::EmbeddingModel> = Vec::new();
        let mut model_names = Vec::new();
        for model in config.available_models.clone() {
            model_names.push(model.model_kind.to_string());
            info!(
                "loading embedding model: {:?}",
                model.model_kind.to_string()
            );
            match model.model_kind {
                AllMiniLmL12V2 | T5Base | AllMiniLmL6V2 => {
                    sentence_transformers.push(model.clone());
                }
                OpenAIAda02 => {
                    let openai_config = config.openai.clone().ok_or(
                        EmbeddingGeneratorError::ConfigurationError(
                            "openai".into(),
                            "openai_config".into(),
                        ),
                    )?;
                    let openai_ada02 = OpenAI::new(openai_config, model.clone())?;
                    router.insert(model.model_kind.to_string(), Arc::new(openai_ada02));
                }
            }
        }
        let sentence_transformer_router = Arc::new(SentenceTransformerModels::new(
            sentence_transformers.clone(),
        )?);
        for st in sentence_transformers {
            router.insert(
                st.model_kind.to_string(),
                sentence_transformer_router.clone(),
            );
        }
        Ok(Self {
            router,
            model_names,
        })
    }

    pub fn list_models(&self) -> Vec<String> {
        self.model_names.clone()
    }
}

#[async_trait]
impl EmbeddingGenerator for EmbeddingRouter {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
        model: String,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        let embedding_model = self
            .router
            .get(&model)
            .ok_or(EmbeddingGeneratorError::ModelNotFound(model.clone()))?;
        embedding_model.generate_embeddings(inputs, model).await
    }

    fn dimensions(&self, model: String) -> Result<u64, EmbeddingGeneratorError> {
        let embedding_model = self
            .router
            .get(&model)
            .ok_or(EmbeddingGeneratorError::ModelLoadingError(model.clone()))?;
        embedding_model.dimensions(model)
    }
}
