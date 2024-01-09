use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};

use crate::{
    api::{self},
    coordinator_client::CoordinatorClient,
    extractor::ExtractedEmbeddings,
    extractor_router::ExtractorRouter,
    grpc_helper::GrpcHelper,
    index::IndexError,
    indexify_coordinator::{self, CreateIndexRequest, Index},
    internal_api::EmbeddingSchema,
    vectordbs::{CreateIndexParams, IndexDistance, VectorChunk, VectorDBTS},
};

pub struct VectorIndexManager {
    coordinator_client: Arc<CoordinatorClient>,
    vector_db: VectorDBTS,
    extractor_router: ExtractorRouter,
}

impl fmt::Debug for VectorIndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIndexManager").finish()
    }
}

pub struct ScoredText {
    pub text: String,
    pub content_id: String,
    pub metadata: HashMap<String, serde_json::Value>,
    pub confidence_score: f32,
}

impl VectorIndexManager {
    pub fn new(coordinator_client: Arc<CoordinatorClient>, vector_db: VectorDBTS) -> Self {
        let extractor_router = ExtractorRouter::new(coordinator_client.clone());
        Self {
            coordinator_client,
            vector_db,
            extractor_router,
        }
    }

    pub async fn create_index(
        &self,
        repository: &str,
        index_name: &str,
        extractor_name: &str,
        schema: EmbeddingSchema,
    ) -> Result<String> {
        let vector_index_name = format!("{}-{}", repository, index_name);
        let create_index_params = CreateIndexParams {
            vectordb_index_name: vector_index_name.clone(),
            vector_dim: schema.dim as u64,
            distance: IndexDistance::from_str(schema.distance.as_str())?,
            unique_params: None,
        };
        self.vector_db.create_index(create_index_params).await?;
        let index = CreateIndexRequest {
            index: Some(Index {
                name: index_name.to_string(),
                table_name: vector_index_name.clone(),
                repository: repository.to_string(),
                schema: serde_json::to_value(schema).unwrap().to_string(),
                extractor: extractor_name.to_string(),
            }),
        };
        let req = GrpcHelper::into_req(index);
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_index(req)
            .await?;

        Ok(vector_index_name.to_string())
    }

    pub async fn add_embedding(
        &self,
        vector_index_name: &str,
        embeddings: Vec<ExtractedEmbeddings>,
    ) -> Result<()> {
        let mut vector_chunks = Vec::new();
        embeddings.iter().for_each(|embedding| {
            let vector_chunk =
                VectorChunk::new(embedding.content_id.clone(), embedding.embeddings.clone());
            vector_chunks.push(vector_chunk);
        });
        self.vector_db
            .add_embedding(vector_index_name, vector_chunks)
            .await?;
        Ok(())
    }

    pub async fn search(
        &self,
        repository: &str,
        index: &str,
        query: &str,
        k: usize,
    ) -> Result<Vec<ScoredText>> {
        let req = indexify_coordinator::GetIndexRequest {
            repository: repository.to_string(),
            name: index.to_string(),
        };
        let index = self
            .coordinator_client
            .get()
            .await?
            .get_index(req)
            .await?
            .into_inner()
            .index
            .ok_or(anyhow!("Index not found"))?;
        let content = api::Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: query.as_bytes().into(),
            feature: None,
            metadata: HashMap::new(),
        };
        let content = self
            .extractor_router
            .extract_content(&index.extractor, content, None)
            .await
            .map_err(|e| IndexError::QueryEmbedding(e.to_string()))?
            .pop()
            .ok_or(anyhow!("No content was extracted"))?;
        let features = content
            .feature
            .as_ref()
            .ok_or(anyhow!("No features were extracted"))?;
        let embedding: Vec<f32> =
            serde_json::from_value(features.data.clone()).map_err(|e| anyhow!(e.to_string()))?;
        let results = self
            .vector_db
            .search(index.table_name, embedding, k as u64)
            .await?;
        let mut index_search_results = Vec::new();
        for result in results {
            let chunk = "dummy text".to_string();
            let search_result = ScoredText {
                text: chunk,
                content_id: result.content_id.clone(),
                metadata: HashMap::new(),
                confidence_score: result.confidence_score,
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}

//#[cfg(test)]
//mod tests {
//
//    use std::{collections::HashMap, env};
//
//    use crate::{
//        blob_storage::BlobStorageBuilder,
//        data_repository_manager::DataRepositoryManager,
//        persistence::{ContentPayload, DataRepository, ExtractorBinding},
//        test_util,
//        test_util::db_utils::{
//            create_index_manager,
//            DEFAULT_TEST_EXTRACTOR,
//            DEFAULT_TEST_REPOSITORY,
//        },
//    };
//
//    #[tokio::test]
//    #[tracing_test::traced_test]
//    async fn test_index_search_basic() {
//        env::set_var("RUST_LOG", "debug");
//        let db = test_util::db_utils::create_db().await.unwrap();
//        let (index_manager, extractor_executor, coordinator) =
//            create_index_manager(db.clone()).await;
//        let blob_storage =
//
// BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).
// unwrap();        let repository_manager =
//            DataRepositoryManager::new_with_db(db.clone(),
// index_manager.clone(), blob_storage);        let _ = repository_manager
//            .create(&DataRepository {
//                name: DEFAULT_TEST_REPOSITORY.into(),
//                data_connectors: vec![],
//                metadata: HashMap::new(),
//                extractor_bindings: vec![ExtractorBinding::new(
//                    "test_extractor_binding",
//                    DEFAULT_TEST_REPOSITORY,
//                    DEFAULT_TEST_EXTRACTOR.into(),
//                    vec![],
//                    serde_json::json!({"a": 1, "b": "hello"}),
//                )],
//            })
//            .await;
//
//        repository_manager
//            .add_texts(
//                DEFAULT_TEST_REPOSITORY,
//                vec![
//                    ContentPayload::from_text(
//                        DEFAULT_TEST_REPOSITORY,
//                        "hello world",
//                        HashMap::new(),
//                    ),
//                    ContentPayload::from_text(
//                        DEFAULT_TEST_REPOSITORY,
//                        "hello pipe",
//                        HashMap::new(),
//                    ),
//                    ContentPayload::from_text(DEFAULT_TEST_REPOSITORY, "nba",
// HashMap::new()),                ],
//            )
//            .await
//            .unwrap();
//        repository_manager
//            .add_texts(
//                DEFAULT_TEST_REPOSITORY,
//                vec![ContentPayload::from_text(
//                    DEFAULT_TEST_REPOSITORY,
//                    "hello world",
//                    HashMap::new(),
//                )],
//            )
//            .await
//            .unwrap();
//
//        coordinator.process_and_distribute_work().await.unwrap();
//        let executor_id = extractor_executor.get_executor_info().id;
//        let work_list = coordinator
//            .shared_state
//            .tasks_for_executor(&executor_id)
//            .await
//            .unwrap();
//
//        //extractor_executor.sync_repo_test(work_list).await.unwrap();
//
//        // FIX ME - This is broken because the Test Setup doesn't start the
//        // coordinator and executor server which we rely to get the
//        // embeddings of the query
//
//        //let result = index_manager
//        //    .search(DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR, "pipe",
//        // 1) .await .unwrap();
//        //assert_eq!(1, result.len())
//    }
//}
//
