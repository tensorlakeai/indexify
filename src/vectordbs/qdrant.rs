use async_trait::async_trait;
use std::collections::HashMap;

use qdrant_client::{
    client::QdrantClient,
    client::{Payload, QdrantClientConfig},
    qdrant::{
        value::{self, Kind},
        vectors_config::Config,
        with_payload_selector::SelectorOptions,
        CountPoints, CreateCollection, Distance, PointStruct, SearchPoints, Value, VectorParams,
        VectorsConfig, WithPayloadSelector,
    },
};

use super::{CreateIndexParams, MetricKind, VectorDb, VectorDbError};
use crate::QdrantConfig;

pub struct QdrantDb {
    qdrant_config: QdrantConfig,
}

impl QdrantDb {
    pub fn new(config: QdrantConfig) -> QdrantDb {
        Self {
            qdrant_config: config,
        }
    }

    async fn create_client(&self) -> Result<QdrantClient, VectorDbError> {
        let client_config = QdrantClientConfig::from_url(&self.qdrant_config.addr);
        let client = QdrantClient::new(Some(client_config))
            .await
            .map_err(|e| VectorDbError::IndexCreationError(e.to_string()))?;
        Ok(client)
    }

    fn to_distance(metric_kind: MetricKind) -> Distance {
        match metric_kind {
            MetricKind::Cosine => Distance::Cosine,
            MetricKind::Dot => Distance::Dot,
            MetricKind::Euclidean => Distance::Euclid,
        }
    }
}

#[async_trait]
impl VectorDb for QdrantDb {
    fn name(&self) -> String {
        "qdrant".into()
    }

    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError> {
        let _collection = self
            .create_client()
            .await?
            .create_collection(&CreateCollection {
                collection_name: index.name,
                vectors_config: Some(VectorsConfig {
                    config: Some(Config::Params(VectorParams {
                        size: index.vector_dim,
                        distance: Self::to_distance(index.metric).into(),
                        hnsw_config: None,
                        quantization_config: None,
                    })),
                }),
                ..Default::default()
            })
            .await
            .map_err(|e| VectorDbError::IndexCreationError(e.to_string()))?;
        Ok(())
    }

    async fn add_embedding(
        &self,
        index: String,
        embeddings: Vec<f32>,
        attrs: HashMap<String, String>,
    ) -> Result<(), VectorDbError> {
        let mut payload = Payload::new();
        for (k, v) in attrs {
            payload.insert(
                k,
                Value {
                    kind: Some(Kind::StringValue(v)),
                },
            );
        }
        let points = vec![PointStruct::new(
            uuid::Uuid::new_v4().to_string(),
            embeddings,
            payload,
        )];
        let _result = self
            .create_client()
            .await?
            .upsert_points(&index, points, None)
            .await
            .map_err(|e| VectorDbError::IndexCreationError(e.to_string()))?;

        let _result = self
            .create_client()
            .await?
            .count(&CountPoints {
                collection_name: index,
                ..Default::default()
            })
            .await
            .unwrap();
        Ok(())
    }

    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<String>, VectorDbError> {
        let result = self
            .create_client()
            .await?
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
            .map_err(|e| VectorDbError::IndexReadError(e.to_string()))?;
        let document_payloads: Vec<&Value> = result
            .result
            .iter()
            .filter_map(|value| value.payload.get("document"))
            .collect();
        let mut documents: Vec<String> = Vec::new();
        for document_payload in document_payloads {
            if let Some(value::Kind::StringValue(doc)) = &document_payload.kind {
                documents.push(doc.clone());
            }
        }
        Ok(documents)
    }

    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let result = self
            .create_client()
            .await?
            .delete_collection(index.clone())
            .await;
        if let Err(err) = result {
            if err.to_string().contains("doesn't exist") {
                return Ok(());
            }
            return Err(VectorDbError::IndexDeletionError(index, err.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::VectorDBTS;

    use super::{CreateIndexParams, QdrantDb};

    #[tokio::test]
    async fn test_qdrant_search_basic() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("hello-index".into()).await.unwrap();
        qdrant
            .create_index(CreateIndexParams {
                name: "hello-index".into(),
                vector_dim: 2,
                metric: crate::MetricKind::Cosine,
            })
            .await
            .unwrap();
        let attrs: HashMap<String, String> = HashMap::from([
            ("document".into(), "hello".into()),
            ("user_id".into(), "5".into()),
        ]);
        qdrant
            .add_embedding("hello-index".into(), vec![0., 2.], attrs)
            .await
            .unwrap();

        let results = qdrant
            .search("hello-index".into(), vec![10., 8.], 1)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }
}
