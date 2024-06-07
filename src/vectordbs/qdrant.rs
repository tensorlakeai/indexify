use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use indexify_internal_api::ContentMetadata;
use qdrant_client::{
    client::{QdrantClient, QdrantClientConfig},
    qdrant::{
        point_id::PointIdOptions::Num,
        points_selector::PointsSelectorOneOf,
        r#match::MatchValue,
        vectors::VectorsOptions,
        vectors_config::Config,
        with_payload_selector::SelectorOptions,
        Condition,
        CreateCollection,
        Distance,
        Filter,
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
use serde::{Deserialize, Serialize};

use super::{CreateIndexParams, VectorDb};
use crate::{
    server_config::QdrantConfig,
    vectordbs::{FilterOperator, IndexDistance, SearchResult, VectorChunk},
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
fn extract_metadata_from_payload(
    payload: HashMap<String, qdrant_client::qdrant::Value>,
) -> Result<(HashMap<String, serde_json::Value>, IndexifyPayload)> {
    let payload: serde_json::Value =
        serde_json::to_value(payload).map_err(|e| anyhow!("{}", e.to_string()))?;
    let mut payload: HashMap<String, serde_json::Value> =
        serde_json::from_value(payload.clone()).map_err(|e| anyhow!(e.to_string()))?;
    let indexify_payload = payload
        .remove("indexify_payload")
        .ok_or(anyhow!("no indexify system payload found"))?;
    let indexify_payload: IndexifyPayload =
        serde_json::from_value(indexify_payload.clone()).map_err(|e| anyhow!(e))?;
    Ok((payload, indexify_payload))
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

#[derive(Debug, Serialize, Deserialize)]
struct IndexifyPayload {
    pub content_metadata: ContentMetadata,
    pub root_content_metadata: Option<ContentMetadata>,
}

impl IndexifyPayload {
    pub fn new(
        content_metadata: ContentMetadata,
        root_content_metadata: Option<ContentMetadata>,
    ) -> Self {
        Self {
            content_metadata,
            root_content_metadata,
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
                        datatype: None,
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
            let mut metadata_map = chunk.metadata.clone();
            let indexify_payload = serde_json::to_value(IndexifyPayload::new(
                chunk.content_metadata.clone(),
                chunk.root_content_metadata.clone(),
            ))
            .map_err(|e| anyhow!("unable to serialize metadata: {}", e.to_string()))?;
            metadata_map.insert("indexify_payload".to_string(), indexify_payload);
            let metadata_map = serde_json::to_value(metadata_map)
                .map_err(|e| anyhow!("unable to serialize metadata: {}", e.to_string()))?;
            let metadata = serde_json::from_value(metadata_map).map_err(|e| {
                anyhow!(
                    "unable to create qdrant payload from metadata: {}",
                    e.to_string()
                )
            })?;
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
            let (metadata, indexify_payload) = extract_metadata_from_payload(point.payload)?;
            let vector = point.vectors.unwrap().vectors_options.unwrap(); // Unwrap the Option<VectorsOptions>
            let embedding = match vector {
                VectorsOptions::Vector(vector) => vector,
                _ => return Err(anyhow!("Invalid vector type")),
            };
            documents.push(VectorChunk {
                content_id: content_id_from_point_id(point.id)?,
                embedding: embedding.data,
                metadata,
                root_content_metadata: indexify_payload.root_content_metadata,
                content_metadata: indexify_payload.content_metadata,
            });
        }
        Ok(documents)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let metadata = serde_json::to_value(metadata)
            .map_err(|e| anyhow!("unable to serialize metadata: {}", e.to_string()))?;
        let metadata = serde_json::from_value(metadata)
            .map_err(|e| anyhow!("unable to read metadata: {}", e.to_string()))?;
        let point_id = hex_to_u64(&content_id).unwrap();
        let points: Vec<PointId> = vec![point_id.into()];
        let _result = self
            .create_client()?
            .set_payload(&index, None, &points.into(), metadata, None, None)
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
        filters: Vec<super::Filter>,
    ) -> Result<Vec<SearchResult>> {
        let mut filter = None;
        if !filters.is_empty() {
            let mut must = Vec::new();
            let mut must_not = Vec::new();

            for f in filters {
                // Qdrant MatchValue doesn't support float values.
                let value: MatchValue = match f.value {
                    serde_json::Value::String(s) => MatchValue::Text(s),
                    serde_json::Value::Bool(b) => MatchValue::Boolean(b),
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            MatchValue::Integer(n.as_i64().unwrap())
                        } else {
                            return Err(anyhow!("Unsupported float type in filter value"));
                        }
                    }
                    _ => return Err(anyhow!("Unsupported type in filter value")),
                };

                match f.operator {
                    FilterOperator::Eq => must.push(Condition::matches(f.key, value)),
                    FilterOperator::Neq => must_not.push(Condition::matches(f.key, value)),
                }
            }
            filter = Some(Filter {
                must,
                must_not,
                ..Default::default()
            });
        }
        let result = self
            .create_client()?
            .search_points(&SearchPoints {
                collection_name: index,
                vector: query_embedding,
                limit: k,
                with_payload: Some(WithPayloadSelector {
                    selector_options: Some(SelectorOptions::Enable(true)),
                }),
                filter,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("unable to read index: {}", e.to_string()))?;
        let mut documents: Vec<SearchResult> = Vec::new();
        for point in result.result {
            let (metadata, indexify_payload) = extract_metadata_from_payload(point.payload)?;
            // TODO similarity score
            documents.push(SearchResult {
                confidence_score: point.score,
                content_id: content_id_from_point_id(point.id)?,
                metadata,
                content_metadata: indexify_payload.content_metadata.clone(),
                root_content_metadata: indexify_payload.root_content_metadata.clone(),
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

    use super::{CreateIndexParams, QdrantDb};
    use crate::{
        server_config::QdrantConfig,
        vectordbs::{
            tests::{basic_search, insertion_idempotent, search_filters, store_metadata},
            IndexDistance,
            VectorDBTS,
        },
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("hello-index").await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        basic_search(qdrant, "hello-index").await;
    }

    #[tokio::test]
    async fn test_store_metadata() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("metadata-index").await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: "metadata-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        store_metadata(qdrant, "metadata-index").await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "idempotent-index";
        let hash_on = vec!["user_id".to_string(), "url".to_string()];
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index(index_name).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        insertion_idempotent(qdrant, index_name).await;
    }

    #[tokio::test]
    async fn test_search_filters() {
        let index_name = "metadata-index";
        let vector_db: VectorDBTS = Arc::new(QdrantDb::new(QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        vector_db.drop_index("metadata-index").await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: "metadata-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        search_filters(vector_db, index_name).await;
    }
}
