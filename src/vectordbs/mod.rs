use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use indexify_internal_api::ContentMetadata;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

use crate::server_config::{IndexStoreKind, VectorIndexConfig};

pub mod lancedb;
//pub mod open_search;
pub mod pg_vector;
pub mod qdrant;

use qdrant::QdrantDb;

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
    pub metadata: HashMap<String, serde_json::Value>,
    pub root_content_metadata: Option<ContentMetadata>,
    pub content_metadata: ContentMetadata,
}

pub type VectorDBTS = Arc<dyn VectorDb + Sync + Send>;

#[derive(Debug, Clone)]
pub struct VectorChunk {
    pub content_id: String,
    pub embedding: Vec<f32>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub root_content_metadata: Option<ContentMetadata>,
    pub content_metadata: ContentMetadata,
}
impl VectorChunk {
    pub fn new(
        content_id: String,
        embedding: Vec<f32>,
        metadata: HashMap<String, serde_json::Value>,
        root_content: Option<ContentMetadata>,
        content_metadata: &ContentMetadata,
    ) -> Self {
        Self {
            content_id,
            embedding,
            metadata,
            root_content_metadata: root_content,
            content_metadata: content_metadata.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FilterOperator {
    Eq,
    Neq,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub key: String,
    pub value: serde_json::Value,
    pub operator: FilterOperator,
}

impl Filter {
    pub fn from_str(filter: &str) -> Result<Self> {
        // FIXME this is a very basic implementation, we should add more operators
        let parts: Vec<&str> = filter.split('=').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid filter: {}", filter));
        }
        let key = parts[0].to_string();
        let value = serde_json::json!(parts[1]);
        let operator = FilterOperator::Eq;
        Ok(Self {
            key,
            value,
            operator,
        })
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
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<()>;

    /// Searches for the nearest neighbors of a query vector in the specified
    /// index.
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
        filters: Vec<Filter>,
    ) -> Result<Vec<SearchResult>>;

    /// Deletes the specified vector index from the vector database.
    async fn drop_index(&self, index: &str) -> Result<()>;

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
            pg_vector::PgVector::new(config.pg_vector_config.unwrap()).await?,
        )),
        // TODO Bring it back
        //IndexStoreKind::OpenSearchKnn => Ok(Arc::new(OpenSearchKnn::new(
        //    config.open_search_basic.unwrap(),
        //))),
        IndexStoreKind::Lancedb => Ok(Arc::new(
            lancedb::LanceDb::new(&config.lancedb_config.unwrap()).await?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{Filter, FilterOperator, VectorDBTS};
    use crate::{
        data_manager::DataManager,
        test_util::db_utils::{create_metadata, test_mock_content_metadata},
        vectordbs::VectorChunk,
    };

    pub async fn crud_operations(vector_db: VectorDBTS, index_name: &str) {
        let content_id = "0";
        let chunk = VectorChunk {
            content_id: content_id.into(),
            embedding: vec![0., 2.],
            metadata: create_metadata(vec![("key1", "value1"), ("key2", "value2")]),
            root_content_metadata: Some(test_mock_content_metadata(content_id, "1", "graph1")),
            content_metadata: test_mock_content_metadata(content_id, "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 1);

        vector_db
            .remove_embedding(index_name, content_id)
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 0);
    }

    pub async fn basic_search(vector_db: VectorDBTS, index_name: &str) {
        let metadata1 = create_metadata(vec![("key1", "value1"), ("key2", "value2")]);
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
            root_content_metadata: Some(test_mock_content_metadata("0", "1", "graph1")),
            content_metadata: test_mock_content_metadata("0", "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();

        let results = vector_db
            .search(index_name.into(), vec![10., 8.], 1, vec![])
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    fn make_id() -> String {
        DataManager::make_id()
    }

    pub async fn store_metadata(vector_db: VectorDBTS, index_name: &str) {
        let content_ids = vec![make_id(), make_id()];
        let metadata1 = create_metadata(vec![("key1", "value1"), ("key2", "value2")]);
        let chunk1 = VectorChunk {
            content_id: content_ids[0].clone(),
            embedding: vec![0.1, 0.2],
            metadata: metadata1.clone(),
            root_content_metadata: Some(test_mock_content_metadata("0", "1", "graph1")),
            content_metadata: test_mock_content_metadata("0", "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk1])
            .await
            .unwrap();
        let metadata2 = create_metadata(vec![("key1", "value3"), ("key2", "value4")]);
        let chunk2 = VectorChunk {
            content_id: content_ids[1].clone(),
            embedding: vec![0.3, 0.4],
            metadata: metadata2.clone(),
            root_content_metadata: Some(test_mock_content_metadata("0", "1", "graph1")),
            content_metadata: test_mock_content_metadata("0", "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk2])
            .await
            .unwrap();

        let result = vector_db
            .get_points(index_name, content_ids.clone())
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for chunk in result {
            if chunk.content_id == content_ids[0] {
                assert_eq!(chunk.metadata, metadata1);
            } else if chunk.content_id == content_ids[1] {
                assert_eq!(chunk.metadata, metadata2);
            } else {
                panic!("unexpected content_id: {}", chunk.content_id);
            }
        }

        let new_metadata = create_metadata(vec![("key1", "value5"), ("key2", "value6")]);
        vector_db
            .update_metadata(index_name, content_ids[0].clone(), new_metadata.clone())
            .await
            .unwrap();
        let result = vector_db
            .get_points(index_name, vec![content_ids[0].clone()])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata, new_metadata);
    }

    pub async fn insertion_idempotent(vector_db: VectorDBTS, index_name: &str) {
        let metadata1 = HashMap::from([
            ("key1".to_string(), json!("value1")),
            ("key2".to_string(), json!("value2")),
        ]);
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
            root_content_metadata: Some(test_mock_content_metadata("0", "1", "graph1")),
            content_metadata: test_mock_content_metadata("0", "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
    }

    pub async fn search_filters(vector_db: VectorDBTS, index_name: &str) {
        let content_ids = vec![make_id(), make_id()];
        let metadata1 = create_metadata(vec![("key1", "value1"), ("key2", "value2")]);
        let chunk = VectorChunk {
            content_id: content_ids[0].clone(),
            embedding: vec![0., 2.],
            metadata: metadata1,
            root_content_metadata: Some(test_mock_content_metadata(&content_ids[0], "1", "graph1")),
            content_metadata: test_mock_content_metadata(&content_ids[0], "1", "graph1"),
        };
        let metadata2 = create_metadata(vec![("key1", "value3"), ("key2", "value4")]);
        let chunk1 = VectorChunk {
            content_id: content_ids[1].clone(),
            embedding: vec![0., 3.],
            metadata: metadata2,
            root_content_metadata: Some(test_mock_content_metadata(&content_ids[1], "1", "graph1")),
            content_metadata: test_mock_content_metadata(&content_ids[1], "1", "graph1"),
        };
        vector_db
            .add_embedding(index_name, vec![chunk, chunk1])
            .await
            .unwrap();

        let res = vector_db
            .search(
                index_name.to_string(),
                vec![0., 2.],
                2,
                vec![Filter {
                    key: "key1".to_string(),
                    value: serde_json::json!("value1"),
                    operator: FilterOperator::Eq,
                }],
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().content_id, content_ids[0]);

        let res = vector_db
            .search(
                index_name.to_string(),
                vec![0., 2.],
                2,
                vec![Filter {
                    key: "key1".to_string(),
                    value: serde_json::json!("value1"),
                    operator: FilterOperator::Neq,
                }],
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().content_id, content_ids[1]);

        let res = vector_db
            .search(
                index_name.to_string(),
                vec![0., 2.],
                2,
                vec![
                    Filter {
                        key: "key1".to_string(),
                        value: serde_json::json!("value1"),
                        operator: FilterOperator::Neq,
                    },
                    Filter {
                        key: "key2".to_string(),
                        value: serde_json::json!("value4"),
                        operator: FilterOperator::Eq,
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res.first().unwrap().content_id, content_ids[1]);

        let res = vector_db
            .search(
                index_name.to_string(),
                vec![0., 2.],
                2,
                vec![
                    Filter {
                        key: "key1".to_string(),
                        value: serde_json::json!("value1"),
                        operator: FilterOperator::Eq,
                    },
                    Filter {
                        key: "key2".to_string(),
                        value: serde_json::json!("value4"),
                        operator: FilterOperator::Eq,
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(res.len(), 0);

        assert_eq!(
            vector_db
                .search(index_name.to_string(), vec![0., 2.], 2, vec![])
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
