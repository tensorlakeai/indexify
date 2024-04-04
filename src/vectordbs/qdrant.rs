use anyhow::{anyhow, Result};
use async_trait::async_trait;
use qdrant_client::{
    client::{QdrantClient, QdrantClientConfig},
    qdrant::{
        point_id::PointIdOptions::Num,
        points_selector::PointsSelectorOneOf,
        vectors::VectorsOptions,
        vectors_config::Config,
        with_payload_selector::SelectorOptions,
        CreateCollection,
        Distance,
        PointId,
        PointStruct,
        PointsIdsList,
        PointsSelector,
        SearchPoints,
        VectorParams,
        VectorsConfig,
        WithPayloadSelector,
    },
};

use super::{CreateIndexParams, VectorDb};
use crate::{
    server_config::QdrantConfig,
    vectordbs::{IndexDistance, SearchResult, VectorChunk},
};

fn hex_to_u64(hex: &str) -> Result<u64, std::num::ParseIntError> {
    u64::from_str_radix(hex, 16)
}

#[allow(dead_code)]
fn u64_to_hex(number: u64) -> String {
    format!("{:x}", number)
}

#[derive(Debug)]
pub struct QdrantDb {
    qdrant_config: QdrantConfig,
}

impl QdrantDb {
    pub fn new(config: QdrantConfig) -> QdrantDb {
        Self {
            qdrant_config: config,
        }
    }

    fn create_client(&self) -> Result<QdrantClient> {
        let client_config = QdrantClientConfig::from_url(&self.qdrant_config.addr);
        let client = QdrantClient::new(Some(client_config))
            .map_err(|e| anyhow!("unable to create a new quadrant index: {}", e))?;
        Ok(client)
    }

    fn convert_to_qdrant_distance(distance: IndexDistance) -> Distance {
        match distance {
            IndexDistance::Cosine => Distance::Cosine,
            IndexDistance::Dot => Distance::Dot,
            IndexDistance::Euclidean => Distance::Euclid,
        }
    }
}

fn content_id_from_point_id(point_id: Option<PointId>) -> Result<String> {
    if let Some(PointId {
        point_id_options: Some(Num(id)),
    }) = point_id
    {
        Ok(format!("{:x}", id))
    } else {
        Err(anyhow!("Invalid point id"))
    }
}

#[async_trait]
impl VectorDb for QdrantDb {
    fn name(&self) -> String {
        "qdrant".into()
    }

    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<()> {
        let result = self
            .create_client()?
            .create_collection(&CreateCollection {
                collection_name: index.vectordb_index_name,
                vectors_config: Some(VectorsConfig {
                    config: Some(Config::Params(VectorParams {
                        on_disk: None,
                        size: index.vector_dim,
                        distance: Self::convert_to_qdrant_distance(index.distance).into(),
                        hnsw_config: None,
                        quantization_config: None,
                    })),
                }),
                ..Default::default()
            })
            .await;
        if let Err(err) = &result {
            if err.to_string().contains("already exists") {
                return Ok(());
            }
        }
        result
            .map(|_| ())
            .map_err(|e| anyhow!("unable to create index: {}", e.to_string()))
    }

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        let mut points = Vec::<PointStruct>::new();
        for chunk in chunks {
            let chunk_id = chunk.content_id.clone();
            let metadata = serde_json::from_value(chunk.metadata.clone())
                .map_err(|e| anyhow!("unable to read metadata: {}", e.to_string()))?;
            points.push(PointStruct::new(
                hex_to_u64(&chunk_id).unwrap(),
                chunk.embedding.clone(),
                metadata,
            ));
        }
        let _result = self
            .create_client()?
            .upsert_points(&index, None, points, None)
            .await
            .map_err(|e| anyhow!("unable to add embedding: {}", e.to_string()))?;
        Ok(())
    }

    async fn get_points(&self, index: &str, ids: Vec<String>) -> Result<Vec<VectorChunk>> {
        let mut points = Vec::<PointId>::new();
        for id in ids {
            let point_id = hex_to_u64(&id).unwrap();
            points.push(PointId {
                point_id_options: Some(Num(point_id)),
            });
        }
        let client = self.create_client()?;

        let result = client
            .get_points(&index, None, &points, Some(true), Some(true), None)
            .await
            .map_err(|e| anyhow!("unable to read index: {}", e.to_string()))?;
        let mut documents: Vec<VectorChunk> = Vec::new();
        for point in result.result {
            let metadata = serde_json::to_value(point.payload)
                .map_err(|e| anyhow!("unable to read metadata: {}", e.to_string()))?;
            let vector = point.vectors.unwrap().vectors_options.unwrap(); // Unwrap the Option<VectorsOptions>
            let embedding = match vector {
                VectorsOptions::Vector(vector) => vector,
                _ => return Err(anyhow!("Invalid vector type")),
            };
            documents.push(VectorChunk {
                content_id: content_id_from_point_id(point.id)?,
                embedding: embedding.data,
                metadata,
            });
        }
        Ok(documents)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: serde_json::Value,
    ) -> Result<()> {
        let metadata = serde_json::from_value(metadata)
            .map_err(|e| anyhow!("unable to read metadata: {}", e.to_string()))?;
        let point_id = hex_to_u64(&content_id).unwrap();
        let points: Vec<PointId> = vec![point_id.into()];
        let _result = self
            .create_client()?
            .overwrite_payload(&index, None, &points.into(), metadata, None)
            .await
            .map_err(|e| anyhow!("unable to update metadata: {}", e.to_string()))?;
        Ok(())
    }

    #[tracing::instrument]
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()> {
        let points_selector: PointsSelector = PointsSelector {
            points_selector_one_of: Some(PointsSelectorOneOf::Points(PointsIdsList {
                ids: vec![hex_to_u64(content_id).unwrap().into()],
            })),
        };
        self.create_client()?
            .delete_points_blocking(index, None, &points_selector, None)
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to remove embedding: {} for index: {}: {}",
                    content_id,
                    index,
                    e.to_string()
                )
            })?;

        Ok(())
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>> {
        let result = self
            .create_client()?
            .search_points(&SearchPoints {
                collection_name: index,
                vector: query_embedding,
                limit: k,
                with_payload: Some(WithPayloadSelector {
                    selector_options: Some(SelectorOptions::Enable(true)),
                }),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("unable to read index: {}", e.to_string()))?;
        let mut documents: Vec<SearchResult> = Vec::new();
        for point in result.result {
            // TODO similarity score
            documents.push(SearchResult {
                confidence_score: point.score,
                content_id: content_id_from_point_id(point.id)?,
            });
        }
        Ok(documents)
    }

    #[tracing::instrument]
    async fn drop_index(&self, index: &str) -> Result<()> {
        let result = self.create_client()?.delete_collection(index).await;
        if let Err(err) = result {
            if err.to_string().contains("doesn't exist") {
                return Ok(());
            }
            return Err(anyhow!(
                "unable to drop {}, err: {}",
                index,
                err.to_string()
            ));
        }
        Ok(())
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let result = self
            .create_client()?
            .collection_info(index)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        let collection_info = result.result.ok_or(anyhow!("index not found: {}", index))?;
        Ok(collection_info.points_count.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::{CreateIndexParams, QdrantDb};
    use crate::{
        data_manager::DataManager,
        server_config::QdrantConfig,
        vectordbs::{IndexDistance, VectorChunk, VectorDBTS},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("hello-index".into()).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
        };
        qdrant
            .add_embedding("hello-index", vec![chunk])
            .await
            .unwrap();

        let results = qdrant
            .search("hello-index".into(), vec![10., 8.], 1)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    fn make_id() -> String {
        DataManager::make_id("namespace", &nanoid::nanoid!(), &None)
    }

    #[tokio::test]
    async fn test_store_metadata() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("metadata-index".into()).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: "metadata-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let content_ids = vec![make_id(), make_id()];
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk1 = VectorChunk {
            content_id: content_ids[0].clone(),
            embedding: vec![0.1, 0.2],
            metadata: metadata1.clone(),
        };
        qdrant
            .add_embedding("metadata-index", vec![chunk1])
            .await
            .unwrap();
        let metadata2 = json!({"key1": "value3", "key2": "value4"});
        let chunk2 = VectorChunk {
            content_id: content_ids[1].clone(),
            embedding: vec![0.3, 0.4],
            metadata: metadata2.clone(),
        };
        qdrant
            .add_embedding("metadata-index", vec![chunk2])
            .await
            .unwrap();

        let result = qdrant
            .get_points("metadata-index", content_ids.clone())
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

        let new_metadata = json!({"key1": "value5", "key2": "value6"});
        qdrant
            .update_metadata(
                "metadata-index",
                content_ids[0].clone(),
                new_metadata.clone(),
            )
            .await
            .unwrap();
        let result = qdrant
            .get_points("metadata-index", vec![content_ids[0].clone()])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata, new_metadata);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "idempotent-index";
        let hash_on = vec!["user_id".to_string(), "url".to_string()];
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index(index_name.into()).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        let metadata1 = json!({"key1": "value1",
    "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
        };
        qdrant
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        qdrant.add_embedding(index_name, vec![chunk]).await.unwrap();
        let num_elements = qdrant.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_deletion() {
        let index_name = "idempotent-index";
        let hash_on = vec!["user_id".to_string(), "url".to_string()];
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index(index_name.into()).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        let content_id = "0";
        let chunk = VectorChunk {
            content_id: content_id.into(),
            embedding: vec![0., 2.],
            metadata: json!({"key1": "value1", "key2": "value2"}),
        };
        qdrant
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        qdrant.add_embedding(index_name, vec![chunk]).await.unwrap();
        let num_elements = qdrant.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);

        qdrant
            .remove_embedding(index_name, content_id)
            .await
            .unwrap();
        let num_elements = qdrant.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 0);
    }
}
