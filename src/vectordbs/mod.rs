use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use sea_orm::{DatabaseConnection, FromQueryResult};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use thiserror::Error;

use crate::server_config::{IndexStoreKind, VectorIndexConfig};

pub mod open_search;
pub mod pg_vector;
pub mod qdrant;

use qdrant::QdrantDb;

use self::{open_search::OpenSearchKnn, pg_vector::PgVector};

#[derive(Display, Debug, Clone, EnumString, Serialize, Deserialize)]
pub enum IndexDistance {
    #[strum(serialize = "cosine")]
    #[serde(rename = "cosine")]
    Cosine,

    #[strum(serialize = "dot")]
    #[serde(rename = "dot")]
    Dot,

    #[strum(serialize = "euclidean")]
    #[serde(rename = "euclidean")]
    Euclidean,
}

/// A request to create a new vector index in the vector database.
#[derive(Clone, Debug)]
pub struct CreateIndexParams {
    pub vectordb_index_name: String,
    pub vector_dim: u64,
    pub distance: IndexDistance,
    // TODO: Probably better if this is a HashMap<String, String> (?), or a generic (?)
    pub unique_params: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, FromQueryResult)]
pub struct SearchResult {
    pub chunk_id: String,
    pub confidence_score: f32,
}

/// An enumeration of possible errors that can occur while interacting with the
/// vector database.
#[derive(Error, Debug)]
pub enum VectorDbError {
    #[error("collection `{0}` has not been deleted: `{1}`")]
    IndexNotDeleted(String, String),

    #[error("error creating index: `{0}`")]
    IndexNotCreated(String),

    #[error("internal error: `{0}")]
    Internal(String),

    #[error("error writing to index: `{0}`")]
    IndexNotWritten(String),

    #[error("error reading from index: `{0}`")]
    IndexNotRead(String),
}

pub type VectorDBTS = Arc<dyn VectorDb + Sync + Send>;

#[derive(Debug, Clone)]
pub struct VectorChunk {
    pub chunk_id: String,
    // TODO should rename this to "embedding"
    pub embeddings: Vec<f32>,
}
impl VectorChunk {
    pub fn new(chunk_id: String, embeddings: Vec<f32>) -> Self {
        Self {
            chunk_id,
            embeddings,
        }
    }
}

/// A trait that defines the interface for interacting with a vector database.
/// The vector database is responsible for storing and querying vector
/// embeddings.
#[async_trait]
pub trait VectorDb {
    /// Creates a new vector index with the specified configuration.
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError>;

    /// Adds a vector embedding to the specified index, along with associated
    /// attributes.
    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError>;

    /// Searches for the nearest neighbors of a query vector in the specified
    /// index.
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>, VectorDbError>;

    /// Deletes the specified vector index from the vector database.
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError>;

    /// Returns the number of vectors in the specified index.
    async fn num_vectors(&self, index: &str) -> Result<u64, VectorDbError>;

    fn name(&self) -> String;
}

/// Creates a new vector database based on the specified configuration.
pub fn create_vectordb(
    config: VectorIndexConfig,
    postgres_db_conn: DatabaseConnection,
) -> Result<VectorDBTS, VectorDbError> {
    match config.index_store {
        IndexStoreKind::Qdrant => Ok(Arc::new(QdrantDb::new(config.qdrant_config.unwrap()))),
        IndexStoreKind::PgVector => Ok(Arc::new(PgVector::new(
            config.pg_vector_config.unwrap(),
            postgres_db_conn,
        ))),
        IndexStoreKind::OpenSearchKnn => Ok(Arc::new(OpenSearchKnn::new(
            config.open_search_basic.unwrap(),
        ))),
    }
}
