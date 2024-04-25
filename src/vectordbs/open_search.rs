use anyhow::{anyhow, Result};
use async_trait::async_trait;
use opensearch::{
    auth::Credentials,
    cert::CertificateValidation,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::IndicesCreateParts,
    BulkOperation,
    OpenSearch,
};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

use super::{CreateIndexParams, VectorDb};
use crate::{
    server_config::OpenSearchBasicConfig,
    vectordbs::{IndexDistance, SearchResult, VectorChunk},
};

pub struct OpenSearchKnn {
    config: OpenSearchBasicConfig,
}

impl OpenSearchKnn {
    pub fn new(config: OpenSearchBasicConfig) -> OpenSearchKnn {
        Self { config }
    }

    fn create_client(&self) -> Result<OpenSearch> {
        let url = Url::parse(&self.config.addr)
            .map_err(|e| anyhow!("unable to parse open search url: {}", e))?;
        let credentials =
            Credentials::Basic(self.config.username.clone(), self.config.password.clone());
        let transport = TransportBuilder::new(SingleNodeConnectionPool::new(url))
            .cert_validation(CertificateValidation::None)
            .auth(credentials)
            .build()
            .map_err(|e| anyhow!("unable to create open search transport: {}", e))?;
        Ok(OpenSearch::new(transport))
    }
}

#[async_trait]
impl VectorDb for OpenSearchKnn {
    fn name(&self) -> String {
        "open search".into()
    }

    async fn create_index(&self, index_params: CreateIndexParams) -> Result<()> {
        let response = self
            .create_client()?
            .indices()
            .create(IndicesCreateParts::Index(&index_params.vectordb_index_name))
            .body(json!(
                {
                    "settings" : {
                        "index": { "knn": true, }
                    },
                    "mappings" : {
                        "properties" : {
                            "embeddings" : {
                                "type" : "knn_vector",
                                "dimension" : index_params.vector_dim as i32,
                                "method": {
                                    "name": "hnsw",
                                    "space_type": match index_params.distance {
                                        IndexDistance::Cosine => "cosinesimil",
                                        IndexDistance::Dot => "innerproduct",
                                        IndexDistance::Euclidean => "l2",
                                    },
                                    "engine": "nmslib"
                                }
                            }
                        }
                    }
                }
            ))
            .send()
            .await
            .map_err(|e| anyhow!("unable to create opensearch index: {}", e))?;
        match response.error_for_status_code() {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(anyhow!("unable to create opensearch index: '{}'", e));
            }
        }
    }

    async fn add_embedding(&self, index_name: &str, vector_chunks: Vec<VectorChunk>) -> Result<()> {
        // TODO: implement smart batching to handle large chunks
        let mut bulk_ops: Vec<opensearch::BulkOperation<Value>> = Vec::new();
        for vector_chunk in vector_chunks {
            let body = json!({
                "embeddings": vector_chunk.embedding,
            });
            bulk_ops.push(BulkOperation::create(vector_chunk.content_id, body).into());
        }

        let response = self
            .create_client()?
            .bulk(opensearch::BulkParts::Index(index_name))
            .body(bulk_ops)
            .send()
            .await
            .map_err(|e| anyhow!("unable to add opensearch embeddings: {}", e))?;

        match response.error_for_status_code() {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(anyhow!("unable to add opensearch embeddings: '{}'", e));
            }
        }
    }

    async fn remove_embedding(&self, index_name: &str, content_id: &str) -> Result<()> {
        let query = json!({
            "query": {
                "match": {
                    "_id": content_id
                }
            }
        });

        let response = self
            .create_client()?
            .delete_by_query(opensearch::DeleteByQueryParts::Index(&[index_name]))
            .body(query)
            .send()
            .await?;

        match response.error_for_status_code() {
            Ok(_) => Ok(()),
            Err(e) => return Err(anyhow!("unable to remove opensearch embeddings: '{}'", e)),
        }
    }

    async fn get_points(&self, _index: &str, _ids: Vec<String>) -> Result<Vec<VectorChunk>> {
        // TODO: return empty vector for now
        Ok(vec![])
    }

    // TODO: implementation of update_metadata
    async fn update_metadata(
        &self,
        _index: &str,
        _content_id: String,
        _metadata: serde_json::Value,
    ) -> Result<()> {
        Ok(())
    }

    async fn search(
        &self,
        index_name: String,
        query_embedding: Vec<f32>,
        k: u64,
        _filters: Vec<super::Filter>,
    ) -> Result<Vec<SearchResult>> {
        let response = self
            .create_client()?
            .search(opensearch::SearchParts::Index(&[&index_name]))
            .body(json!({
                "query": {
                    "knn": {
                        "embeddings": {
                            "vector": query_embedding,
                            "k": k
                        }
                    }
                }
            }))
            .send()
            .await
            .map_err(|e| anyhow!("unable to search opensearch embeddings: {}", e))?;

        let response_body = response
            .json::<Value>()
            .await
            .map_err(|e| anyhow!("unable to parse opensearch search response: {}", e))?;

        let returned_hits = response_body["hits"]["hits"].as_array();
        match returned_hits {
            None => {
                return Err(anyhow!(
                    "unable to parse opensearch search response".to_string(),
                ))
            }
            Some(hits) => {
                let mut documents: Vec<SearchResult> = Vec::new();
                for hit in hits {
                    #[derive(Deserialize)]
                    struct OpenSearchHit {
                        _id: String,
                        _score: f64,
                    }

                    let hit = serde_json::from_value::<OpenSearchHit>(hit.clone());
                    match hit {
                        Err(e) => {
                            return Err(anyhow!(
                                "unable to parse opensearch search response: {}",
                                e
                            ));
                        }
                        Ok(hit) => {
                            documents.push(SearchResult {
                                content_id: hit._id,
                                confidence_score: hit._score as f32,
                            });
                        }
                    }
                }
                Ok(documents)
            }
        }
    }

    async fn drop_index(&self, index: &str) -> Result<()> {
        let response = self
            .create_client()?
            .indices()
            .delete(opensearch::indices::IndicesDeleteParts::Index(&[&index]))
            .send()
            .await
            .map_err(|e| anyhow!("unable to delete opensearch index: {}", e))?;

        match response.error_for_status_code() {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(status) = e.status_code() {
                    if status.as_u16() == 404 {
                        return Ok(());
                    }
                }
                return Err(anyhow!("unable to delete opensearch index: '{}'", e));
            }
        }
    }

    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let response = self
            .create_client()?
            .count(opensearch::CountParts::Index(&[&index]))
            .send()
            .await
            .map_err(|e| anyhow!("unable to count opensearch index: {}", e))?;

        let response_body = response
            .json::<Value>()
            .await
            .map_err(|e| anyhow!("unable to parse opensearch count response: {}", e))?;

        #[derive(Deserialize)]
        struct OpenSearchCount {
            count: u64,
        }

        let result = serde_json::from_value::<OpenSearchCount>(response_body)
            .map_err(|e| anyhow!("unable to parse opensearch count response: {}", e))?;

        Ok(result.count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        server_config::OpenSearchBasicConfig,
        vectordbs::{IndexDistance, VectorChunk, VectorDBTS},
    };
    const TEST_INDEX_NAME: &str = "test_index_name";

    use super::{CreateIndexParams, OpenSearchKnn};

    fn initialize_opensearch() -> OpenSearchKnn {
        OpenSearchKnn::new(OpenSearchBasicConfig {
            addr: "https://localhost:9200".into(),
            username: "admin".into(),
            password: "admin".into(),
        })
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[ignore]
    async fn test_search_basic() {
        let opensearch: VectorDBTS = Arc::new(initialize_opensearch());
        opensearch.drop_index(TEST_INDEX_NAME).await.unwrap();
        opensearch
            .create_index(CreateIndexParams {
                vectordb_index_name: TEST_INDEX_NAME.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: serde_json::Value::Null,
        };
        opensearch
            .add_embedding(TEST_INDEX_NAME, vec![chunk])
            .await
            .unwrap();

        // bulk api returns a response when the document is accepted for indexing, not
        // when it is actually indexed. So we need to wait until the document is
        // actually indexed before searching.

        const MAX_MILLIS_TO_WAIT: u64 = 2000;
        let mut millis_spent_waiting: u64 = 0;
        const WAIT_MILLIS_PER_ITER: u64 = 10;

        loop {
            if millis_spent_waiting >= MAX_MILLIS_TO_WAIT {
                panic!(
                    "timed out waiting for document to be indexed, spent '{}' millisecs waiting",
                    millis_spent_waiting
                );
            }
            let num_elements = opensearch.num_vectors(TEST_INDEX_NAME).await.unwrap();
            if num_elements == 1 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(WAIT_MILLIS_PER_ITER)).await;
            millis_spent_waiting += WAIT_MILLIS_PER_ITER;
        }

        let results = opensearch
            .search(TEST_INDEX_NAME.into(), vec![10., 8.], 1, vec![])
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }
}
