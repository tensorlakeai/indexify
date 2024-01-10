use std::{
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use nanoid::nanoid;
use tracing::info;

pub const DEFAULT_REPOSITORY_NAME: &str = "default";
use crate::{
    api::{self, EmbeddingSchema},
    attribute_index::{AttributeIndexManager, ExtractedAttributes},
    blob_storage::BlobStorageTS,
    coordinator_client::CoordinatorClient,
    extractor::ExtractedEmbeddings,
    grpc_helper::GrpcHelper,
    indexify_coordinator::{self, ContentMetadata, CreateContentRequest, ListIndexesRequest, Index, CreateIndexRequest},
    internal_api::{self, OutputSchema},
    server_config::ServerConfig,
    vector_index::{ScoredText, VectorIndexManager},
};

pub struct DataRepositoryManager {
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
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
        blob_storage: BlobStorageTS,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Result<Self> {
        Ok(Self {
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
            coordinator_client,
        })
    }

    #[allow(dead_code)]
    pub async fn new_with_db(
        db_addr: &str,
        vector_index_manager: Arc<VectorIndexManager>,
        blob_storage: BlobStorageTS,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Result<Self> {
        let attribute_index_manager =
            Arc::new(AttributeIndexManager::new(db_addr, coordinator_client.clone()).await?);
        Ok(Self {
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
            coordinator_client,
        })
    }

    #[tracing::instrument]
    pub async fn create_default_repository(&self, _server_config: &ServerConfig) -> Result<()> {
        let _ = self
            .coordinator_client
            .get()
            .await?
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
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_repositories(req)
            .await?;
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
            .await?
            .create_repository(request)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get(&self, name: &str) -> Result<api::DataRepository> {
        let req = indexify_coordinator::GetRepositoryRequest {
            name: name.to_string(),
        };
        let respsonse = self
            .coordinator_client
            .get()
            .await?
            .get_repository(req)
            .await?
            .into_inner();
        let repository = respsonse
            .repository
            .ok_or(anyhow!("repository not found"))?;
        repository.try_into()
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
        let response = self
            .coordinator_client
            .get()
            .await?
            .create_binding(req)
            .await?
            .into_inner();
        let mut index_names = Vec::new();
        let extractor = response.extractor.ok_or(anyhow!("extractor not found"))?;
        for (name, output_schema) in &extractor.outputs {
            let output_schema: OutputSchema = serde_json::from_str(output_schema)?;
            match output_schema {
                internal_api::OutputSchema::Embedding(embedding_schema) => {
                    let index_name = format!("{}-{}", extractor_binding.name, name);
                    let _ = self
                        .vector_index_manager
                        .create_index(
                            repository,
                            &index_name,
                            embedding_schema.clone(),
                        )
                        .await?;
                    let _ = self
                        .create_index_metadata(
                            repository,
                            &index_name,
                            &index_name,
                            serde_json::to_value(embedding_schema)?,
                            &extractor_binding.name,
                            &extractor.name,
                        )
                        .await?;
                    index_names.push(index_name);
                }
                internal_api::OutputSchema::Attributes(schema) => {
                    let index_name = format!("{}-{}", extractor_binding.name, name);
                    let _ = self
                        .attribute_index_manager
                        .create_index(
                            repository,
                            &index_name,
                            &extractor.name,
                            &extractor_binding.name,
                            schema,
                        )
                        .await?;
                    index_names.push(index_name);
                }
            }
        }

        Ok(index_names)
    }

    async fn create_index_metadata(&self, repository: &str, index_name: &str, vector_index_name: &str, schema: serde_json::Value, binding: &str, extractor: &str) -> Result<()> {
        let index = CreateIndexRequest{
            index: Some(Index{
                name: index_name.to_string(),
                table_name: vector_index_name.to_string(),
                repository: repository.to_string(),
                schema: serde_json::to_value(schema).unwrap().to_string(),
                extractor_binding: binding.to_string(),
                extractor: extractor.to_string(),
            }),
        };
        let req = GrpcHelper::into_req(index);
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_index(req)
            .await?;
        Ok(())

    }

    pub async fn list_content(&self, repository: &str) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::ListContentRequest {
            repository: repository.to_string(),
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_content(req)
            .await?;
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
    pub async fn add_texts(&self, repo_name: &str, content_list: Vec<api::Content>) -> Result<()> {
        for text in content_list {
            let content_metadata = self.write_content(repo_name, text, None).await?;
            let req = CreateContentRequest {
                content: Some(content_metadata),
            };
            self.coordinator_client
                .get()
                .await?
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

    async fn write_content(
        &self,
        repository: &str,
        content: api::Content,
        parent_id: Option<String>,
    ) -> Result<ContentMetadata> {
        let current_ts_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        content.bytes.hash(&mut s);
        let id = format!("{:x}", s.finish());
        let file_name = nanoid!();
        let storage_url = self
            .upload_file(repository, &file_name, Bytes::from(content.bytes.clone()))
            .await
            .map_err(|e| anyhow!("unable to write text to blob store: {}", e))?;
        let labels = content
            .metadata
            .clone()
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();
        Ok(ContentMetadata {
            id,
            file_name,
            storage_url,
            parent_id: parent_id.unwrap_or_default(),
            created_at: current_ts_secs as i64,
            mime: mime::TEXT_PLAIN.to_string(),
            repository: repository.to_string(),
            labels,
        })
    }

    pub async fn write_extracted_content(
        &self,
        extracted_content: api::WriteExtractedContent,
    ) -> Result<()> {
        for content in extracted_content.content_list {
            let content_metadata = self
                .write_content(
                    &extracted_content.repository,
                    content.clone(),
                    Some(extracted_content.parent_content_id.to_string()),
                )
                .await?;
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
        let resp = self
            .coordinator_client
            .get()
            .await?
            .list_indexes(req)
            .await?;
        let indexes = resp
            .into_inner()
            .indexes
            .into_iter()
            .map(|i| api::Index {
                name: i.name,
                schema: api::ExtractorOutputSchema::Embedding(EmbeddingSchema {
                    dim: 384,
                    distance: api::IndexDistance::Cosine,
                }),
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
        let req = indexify_coordinator::GetIndexRequest {
            repository: repository.to_string(),
            name: index_name.to_string(),
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
        self.vector_index_manager
            .search(index, query, k as usize)
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
        let req = indexify_coordinator::ListExtractorsRequest {};
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_extractors(req)
            .await?
            .into_inner();

        let extractors = response
            .extractors
            .into_iter()
            .map(|e| e.try_into())
            .collect::<Result<Vec<api::ExtractorDescription>>>()?;
        Ok(extractors)
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
