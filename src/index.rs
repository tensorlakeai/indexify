use thiserror::Error;
use tracing::error;

use crate::vectordbs::VectorDbError;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    VectorDb(#[from] VectorDbError),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),

    #[error("unable to embed query: `{0}`")]
    QueryEmbedding(String),
}
