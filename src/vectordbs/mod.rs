use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use thiserror::Error;

use crate::server_config;

pub mod qdrant;

use qdrant::QdrantDb;

pub enum MetricKind {
    Dot,
    Euclidean,
    Cosine,
}

pub struct CreateIndexParams {
    pub name: String,
    pub vector_dim: u64,
    pub metric: MetricKind,
}

#[derive(Error, Debug)]
pub enum VectorDbError {
    #[error("collection `{0}` has not been deleted: `{1}`")]
    IndexDeletionError(String, String),

    #[error("config not present")]
    ConfigNotPresent,

    #[error("error creating index: `{0}`")]
    IndexCreationError(String),

    #[error("error writing to index: `{0}`")]
    IndexWriteError(String),

    #[error("error reading from index: `{0}`")]
    IndexReadError(String),
}

pub type VectorDBTS = Arc<dyn VectorDb + Sync + Send>;

#[async_trait]
pub trait VectorDb {
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError>;

    async fn add_embedding(
        &self,
        index: String,
        embeddings: Vec<f32>,
        attrs: HashMap<String, String>,
    ) -> Result<(), VectorDbError>;

    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<String>, VectorDbError>;

    async fn drop_index(&self, index: String) -> Result<(), VectorDbError>;
}

pub fn create_vectordb(
    config: Arc<server_config::ServerConfig>,
) -> Result<Option<VectorDBTS>, VectorDbError> {
    let qdrant = config
        .index_config
        .clone()
        .and_then(|c| c.qdrant_config)
        .map(QdrantDb::new);
    Ok(qdrant)
}
