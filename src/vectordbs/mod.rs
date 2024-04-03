use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::server_config::{IndexStoreKind, VectorIndexConfig};

pub mod lancedb;
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

#[derive(Debug, Default, Clone)]
pub struct SearchResult {
    pub content_id: String,
    pub confidence_score: f32,
}

pub type VectorDBTS = Arc<dyn VectorDb + Sync + Send>;

#[derive(Debug, Clone)]
pub struct VectorChunk {
    pub content_id: String,
    pub embedding: Vec<f32>,
    pub metadata: serde_json::Value,
}
impl VectorChunk {
    pub fn new(content_id: String, embedding: Vec<f32>, metadata: serde_json::Value) -> Self {
        Self {
            content_id,
            embedding,
            metadata,
        }
    }
}

/// A trait that defines the interface for interacting with a vector database.
/// The vector database is responsible for storing and querying vector
/// embeddings.
#[async_trait]
pub trait VectorDb {
    /// Creates a new vector index with the specified configuration.
    async fn create_index(&self, index: CreateIndexParams) -> Result<()>;

    /// Adds a vector embedding to the specified index, along with associated
    /// attributes.
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()>;

    /// Removes a vector embedding from the specified index based on the
    /// content_id key
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()>;

    /// Retrieves the vector embeddings for the specified content IDs
    async fn get_points(&self, index: &str, content_ids: Vec<String>) -> Result<Vec<VectorChunk>>;

    /// Update metadata for the specified content ID
    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: serde_json::Value,
    ) -> Result<()>;

    /// Searches for the nearest neighbors of a query vector in the specified
    /// index.
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>>;

    /// Deletes the specified vector index from the vector database.
    async fn drop_index(&self, index: String) -> Result<()>;

    /// Returns the number of vectors in the specified index.
    async fn num_vectors(&self, index: &str) -> Result<u64>;

    fn name(&self) -> String;

    //  TODO: Add delete content using namespace and content id
}

/// Creates a new vector database based on the specified configuration.
pub async fn create_vectordb(config: VectorIndexConfig) -> Result<VectorDBTS> {
    match config.index_store {
        IndexStoreKind::Qdrant => Ok(Arc::new(QdrantDb::new(config.qdrant_config.unwrap()))),
        IndexStoreKind::PgVector => Ok(Arc::new(
            PgVector::new(config.pg_vector_config.unwrap()).await?,
        )),
        IndexStoreKind::OpenSearchKnn => Ok(Arc::new(OpenSearchKnn::new(
            config.open_search_basic.unwrap(),
        ))),
        IndexStoreKind::Lancedb => Ok(Arc::new(
            lancedb::LanceDb::new(&config.lancedb_config.unwrap()).await?,
        )),
    }
}
