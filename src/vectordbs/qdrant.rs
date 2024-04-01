use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use qdrant_client::{
    client::{Payload, QdrantClient, QdrantClientConfig},
    qdrant::{
        vectors_config::Config,
        with_payload_selector::SelectorOptions,
        CreateCollection,
        Distance,
        PointStruct,
        SearchPoints,
        VectorParams,
        VectorsConfig,
        WithPayloadSelector,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::json;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QdrantPayload {
    pub chunk_id: String,
    pub metadata: serde_json::Value,
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
            let payload: Payload = json!(QdrantPayload {
                chunk_id: chunk_id.clone(),
                metadata: json!(HashMap::<String, String>::new()),
            })
            .try_into()
            .unwrap();
            points.push(PointStruct::new(
                hex_to_u64(&chunk_id).unwrap(),
                chunk.embedding.clone(),
                payload,
            ));
        }
        let _result = self
            .create_client()?
            .upsert_points(&index, None, points, None)
            .await
            .map_err(|e| anyhow!("unable to add embedding: {}", e.to_string()))?;
        Ok(())
    }

    #[tracing::instrument]
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()> {
        //  TODO: Complete this implementaiton for Qdrant to delete rows based on the
        // content_id
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
            let json_value = serde_json::to_value(point.payload)
                .map_err(|e| anyhow!("unable to read embedding: {}", e.to_string()))?;
            let qdrant_payload: QdrantPayload = serde_json::from_value(json_value)
                .map_err(|e| anyhow!("unable to read embedding: {}", e.to_string()))?;
            // TODO similarity score
            documents.push(SearchResult {
                confidence_score: point.score,
                content_id: qdrant_payload.chunk_id,
            });
        }
        Ok(documents)
    }

    #[tracing::instrument]
    async fn drop_index(&self, index: String) -> Result<()> {
        let result = self.create_client()?.delete_collection(index.clone()).await;
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

    use super::{CreateIndexParams, QdrantDb};
    use crate::{
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
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
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
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
        };
        qdrant
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        qdrant.add_embedding(index_name, vec![chunk]).await.unwrap();
        let num_elements = qdrant.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
    }
}
