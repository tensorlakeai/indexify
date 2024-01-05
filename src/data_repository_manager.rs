use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use mime::Mime;
use sea_orm::DbConn;
use tonic::IntoRequest;
use tracing::info;
use tracing_core::span::Current;
use utoipa::openapi::Content;

pub const DEFAULT_REPOSITORY_NAME: &str = "default";
use crate::{
    api,
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageTS,
    extractor::ExtractedEmbeddings,
    grpc_helper::GrpcHelper,
    indexify_coordinator::{self, ContentMetadata, CreateContentRequest, ListIndexesRequest},
    persistence::{ExtractedAttributes, Repository, RepositoryError},
    server_config::ServerConfig,
    service_client::CoordinatorClient,
    vector_index::{ScoredText, VectorIndexManager},
};

pub struct DataRepositoryManager {
    repository: Arc<Repository>,
    vector_index_manager: Arc<VectorIndexManager>,
    attribute_index_manager: Arc<AttributeIndexManager>,
    blob_storage: BlobStorageTS,
    coordinator_client: Arc<CoordinatorClient>,
}

impl fmt::Debug for DataRepositoryManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataRepositoryManager").finish()
    }
}

impl DataRepositoryManager {
    pub async fn new(
        repository: Arc<Repository>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
        blob_storage: BlobStorageTS,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
            coordinator_client,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_db(
        db: DbConn,
        vector_index_manager: Arc<VectorIndexManager>,
        blob_storage: BlobStorageTS,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Self {
        let repository = Arc::new(Repository::new_with_db(db));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(
            repository.clone(),
            coordinator_client.clone(),
        ));
        Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
            coordinator_client,
        }
    }

    #[tracing::instrument]
    pub async fn create_default_repository(&self, _server_config: &ServerConfig) -> Result<()> {
        let _ = self
            .coordinator_client
            .get()
            .create_repository(indexify_coordinator::CreateRepositoryRequest {
                name: DEFAULT_REPOSITORY_NAME.into(),
                bindings: Vec::new(),
            })
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_repositories(&self) -> Result<Vec<api::DataRepository>> {
        let req = indexify_coordinator::ListRepositoriesRequest {};
        let response = self.coordinator_client.get().list_repositories(req).await?;
        let repositories = response.into_inner().repositories;
        let data_respoistories = repositories
            .into_iter()
            .map(|r| api::DataRepository {
                name: r.name,
                extractor_bindings: Vec::new(),
            })
            .collect();
        Ok(data_respoistories)
    }

    #[tracing::instrument]
    pub async fn create(&self, repository: &api::DataRepository) -> Result<()> {
        info!("creating data repository: {}", repository.name);
        let bindings = repository
            .extractor_bindings
            .clone()
            .into_iter()
            .map(|b| b.into())
            .collect();
        let request = indexify_coordinator::CreateRepositoryRequest {
            name: repository.name.clone(),
            bindings,
        };
        let _resp = self
            .coordinator_client
            .get()
            .create_repository(request)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get(&self, name: &str) -> Result<api::DataRepository> {
        let req = indexify_coordinator::GetRepositoryRequest {
            name: name.to_string(),
        };
        let respsonse = self.coordinator_client.get().get_repository(req).await?;
        let repository = respsonse.into_inner().repository.unwrap();
        let data_repository = api::DataRepository {
            name: repository.name,
            extractor_bindings: Vec::new(),
        };
        Ok(data_repository)
    }

    pub async fn add_extractor_binding(
        &self,
        repository: &str,
        extractor_binding: &api::ExtractorBinding,
    ) -> Result<Vec<String>> {
        info!(
            "adding extractor bindings repository: {}, extractor: {}, binding: {}",
            repository, extractor_binding.extractor, extractor_binding.name,
        );
        let req = indexify_coordinator::ExtractorBindRequest {
            repository: repository.to_string(),
            binding: Some(extractor_binding.clone().into()),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64,
        };
        let response = self.coordinator_client.get().create_binding(req).await?;
        Ok(response.into_inner().index_names.clone())
    }

    pub async fn list_content(&self, repository: &str) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::ListContentRequest {
            repository: repository.to_string(),
        };
        let response = self.coordinator_client.get().list_content(req).await?;
        let content_list = response.into_inner().content_list;
        let mut content = Vec::new();
        for c in content_list {
            let metadata = c
                .labels
                .clone()
                .into_iter()
                .map(|(k, v)| (k, serde_json::from_str(&v).unwrap()))
                .collect();
            content.push(api::ContentMetadata {
                id: c.id,
                name: c.file_name,
                storage_url: c.storage_url,
                parent_id: c.parent_id,
                created_at: c.created_at,
                content_type: c.mime,
                repository: c.repository,
                metadata,
            })
        }
        Ok(content)
    }

    #[tracing::instrument]
    pub async fn add_texts(&self, repo_name: &str, texts: Vec<api::Text>) -> Result<()> {
        let current_ts = SystemTime::now();
        let current_ts_ms = current_ts
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();
        let current_ts_secs = current_ts.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
        for (i, text) in texts.iter().enumerate() {
            let mut s = DefaultHasher::new();
            repo_name.hash(&mut s);
            text.text.hash(&mut s);
            let id = format!("{:x}", s.finish());
            let file_name = format!("{}_{}", current_ts_ms, i);
            let storage_url = self
                .upload_file(repo_name, &file_name, Bytes::from(text.text.clone()))
                .await
                .map_err(|e| anyhow!("unable to write text to blob store: {}", e))?;
            let labels = text
                .metadata
                .clone()
                .into_iter()
                .map(|(k, v)| (k, v.to_string()))
                .collect();
            let content_metadata = ContentMetadata {
                id,
                file_name,
                storage_url,
                parent_id: "".to_string(),
                created_at: current_ts_secs as i64,
                mime: mime::TEXT_PLAIN.to_string(),
                repository: repo_name.to_string(),
                labels,
            };
            let req = CreateContentRequest {
                content: Some(content_metadata),
            };

            self.coordinator_client
                .get()
                .create_content(GrpcHelper::into_req(req))
                .await
                .map_err(|e| {
                    anyhow!(
                        "unable to write content metadata to coordinator {}",
                        e.to_string()
                    )
                })?;
        }
        Ok(())
    }

    async fn write_content(&self, content: api::Content) -> Result<ContentMetadata> {
        Err(anyhow!("not implemented"))
    }

    pub async fn write_extracted_content(
        &self,
        extracted_content: api::WriteExtractedContent,
    ) -> Result<()> {
        for content in extracted_content.content_list {
            let content_metadata = self.write_content(content.clone()).await?;
            if let Some(feature) = content.feature.clone() {
                match feature.feature_type {
                    api::FeatureType::Embedding => {
                        let emebedding: Vec<f32> = serde_json::from_value(feature.data)?;
                        let embeddings = ExtractedEmbeddings {
                            content_id: content_metadata.id.to_string(),
                            embeddings: emebedding,
                        };
                        self.vector_index_manager
                            .add_embedding(&extracted_content.index_name, vec![embeddings])
                            .await?;
                    }
                    api::FeatureType::Metadata => {
                        let extracted_attributes = ExtractedAttributes::new(
                            &content_metadata.id,
                            feature.data.clone(),
                            "extractor_name",
                        );
                        self.attribute_index_manager
                            .add_index(
                                &extracted_content.repository,
                                &extracted_content.index_name,
                                extracted_attributes,
                            )
                            .await?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_indexes(&self, repository: &str) -> Result<Vec<api::Index>> {
        let req = ListIndexesRequest {
            repository: repository.to_string(),
        };
        let resp = self.coordinator_client.get().list_indexes(req).await?;
        let indexes = resp
            .into_inner()
            .indexes
            .into_iter()
            .map(|i| api::Index {
                name: i.name,
                schema: api::ExtractorOutputSchema::Embedding {
                    dim: 384,
                    distance: api::IndexDistance::Cosine,
                },
            })
            .collect();
        Ok(indexes)
    }

    #[tracing::instrument]
    pub async fn search(
        &self,
        repository: &str,
        index_name: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<ScoredText>> {
        self.vector_index_manager
            .search(repository, index_name, query, k as usize)
            .await
    }

    #[tracing::instrument]
    pub async fn attribute_lookup(
        &self,
        repository: &str,
        index_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedAttributes>, anyhow::Error> {
        self.attribute_index_manager
            .get_attributes(repository, index_name, content_id)
            .await
    }

    #[tracing::instrument]
    pub async fn list_extractors(&self) -> Result<Vec<api::ExtractorDescription>> {
        //let req = indexify_coordinator::ListEx
        Ok(Vec::new())
    }

    #[tracing::instrument]
    pub async fn upload_file(
        &self,
        repository: &str,
        name: &str,
        file: Bytes,
    ) -> Result<String, anyhow::Error> {
        let stored_file_path = self.blob_storage.put(name, file).await?;
        Ok(stored_file_path)
    }
}

//#[cfg(test)]
//mod tests {
//    use std::collections::HashMap;
//
//    use serde_json::json;
//
//    use super::*;
//    use crate::{
//        blob_storage::BlobStorageBuilder,
//        persistence::{DataConnector, Event, ExtractorBinding, SourceType},
//        test_util,
//        test_util::db_utils::{DEFAULT_TEST_EXTRACTOR,
// DEFAULT_TEST_REPOSITORY},    };
//
//    #[tokio::test]
//    #[tracing_test::traced_test]
//    async fn test_sync_repository() {
//        let db = test_util::db_utils::create_db().await.unwrap();
//        let (index_manager, ..) =
// test_util::db_utils::create_index_manager(db.clone()).await;        let
// blob_storage =
// BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).
// unwrap();        let repository_manager =
//            DataRepositoryManager::new_with_db(db.clone(), index_manager,
// blob_storage);        let mut meta = HashMap::new();
//        meta.insert("foo".to_string(), json!(12));
//        let repository = DataRepository {
//            name: "test".to_string(),
//            extractor_bindings: vec![ExtractorBinding::new(
//                "test_extractor_binding",
//                "test",
//                DEFAULT_TEST_EXTRACTOR.to_string(),
//                vec![],
//                serde_json::json!({}),
//            )],
//            metadata: meta.clone(),
//            data_connectors: vec![DataConnector {
//                source: SourceType::GoogleContact {
//                    metadata: Some("data_connector_meta".to_string()),
//                },
//            }],
//        };
//        repository_manager.create(&repository).await.unwrap();
//        let repositories =
// repository_manager.list_repositories().await.unwrap();        assert_eq!
// (repositories.len(), 1);        assert_eq!(repositories[0].name, "test");
//        assert_eq!(repositories[0].extractor_bindings.len(), 1);
//        assert_eq!(repositories[0].data_connectors.len(), 1);
//        assert_eq!(repositories[0].metadata, meta);
//    }
//
//    #[tokio::test]
//    #[tracing_test::traced_test]
//    async fn test_events() {
//        let db = test_util::db_utils::create_db().await.unwrap();
//        let (index_manager, extractor_executor, coordinator) =
//            test_util::db_utils::create_index_manager(db.clone()).await;
//
//        let blob_storage =
//
// BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).
// unwrap();        let repository_manager =
// Arc::new(DataRepositoryManager::new_with_db(            db.clone(),
//            index_manager.clone(),
//            blob_storage,
//        ));
//        info!("creating repository");
//
//        repository_manager
//            .create(&test_util::db_utils::default_test_data_repository())
//            .await
//            .unwrap();
//
//        let messages: Vec<Event> = vec![
//            Event::new("hello world", None, HashMap::new()),
//            Event::new("hello friend", None, HashMap::new()),
//            Event::new("how are you", None, HashMap::new()),
//        ];
//
//        info!("adding messages to session");
//        repository_manager
//            .add_events(DEFAULT_TEST_REPOSITORY, messages.clone())
//            .await
//            .unwrap();
//
//        let retrieve_result = repository_manager
//            .list_events(DEFAULT_TEST_REPOSITORY)
//            .await
//            .unwrap();
//        assert_eq!(retrieve_result.len(), 3);
//
//        info!("manually syncing messages");
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
//        //let search_results = repository_manager
//        //    .search(DEFAULT_TEST_REPOSITORY, "memory_session_embeddings",
//        // "hello", 2)    .await
//        //    .unwrap();
//        //assert_eq!(search_results.len(), 2);
//    }
//}
//
