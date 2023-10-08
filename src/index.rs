use thiserror::Error;

use tracing::error;

use crate::persistence::RepositoryError;
use crate::vectordbs::VectorDbError;

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    VectorDb(#[from] VectorDbError),

    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),

    #[error("chunk not found: `{0}`")]
    ChunkNotFound(String),

    #[error("unable to embed query: `{0}`")]
    QueryEmbedding(String),
}
