use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use thiserror::Error;

use crate::VectorIndexConfig;

pub mod qdrant;

use qdrant::QdrantDb;

/// The type of distance metric to use when comparing vectors in the vector database.
#[derive(Clone)]
pub enum MetricKind {
    Dot,
    Euclidean,
    Cosine,
}

/// A request to create a new vector index in the vector database.
#[derive(Clone)]
pub struct CreateIndexParams {
    pub name: String,
    pub vector_dim: u64,
    pub metric: MetricKind,
}

/// An enumeration of possible errors that can occur while interacting with the vector database.
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

pub const DOC_PAYLOAD: &str = "___document";

/// A trait that defines the interface for interacting with a vector database.
/// The vector database is responsible for storing and querying vector embeddings.
#[async_trait]
pub trait VectorDb {
    /// Creates a new vector index with the specified configuration.
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError>;

    /// Adds a vector embedding to the specified index, along with associated attributes.
    async fn add_embedding(
        &self,
        index: String,
        embeddings: Vec<f32>,
        attrs: HashMap<String, String>,
    ) -> Result<(), VectorDbError>;

    /// Searches for the nearest neighbors of a query vector in the specified index.
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<String>, VectorDbError>;

    /// Deletes the specified vector index from the vector database.
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError>;

    fn name(&self) -> String;
}

/// Creates a new vector database based on the specified configuration.
pub fn create_vectordb(config: VectorIndexConfig) -> Result<VectorDBTS, VectorDbError> {
    match config.index_store {
        crate::IndexStoreKind::Qdrant => Ok(Arc::new(QdrantDb::new(config.qdrant_config.unwrap()))),
    }
}
