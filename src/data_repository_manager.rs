use std::{collections::HashMap, fmt, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use jsonschema::JSONSchema;
use sea_orm::DbConn;
use thiserror::Error;
use tracing::{error, info};

pub const DEFAULT_REPOSITORY_NAME: &str = "default";

use crate::{
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageTS,
    index::IndexError,
    persistence::{
        ContentPayload,
        DataRepository,
        Event,
        ExtractedAttributes,
        Extractor,
        ExtractorBinding,
        ExtractorOutputSchema,
        Index,
        Repository,
        RepositoryError,
    },
    server_config::ServerConfig,
    vector_index::{ScoredText, VectorIndexManager},
};

#[derive(Error, Debug)]
pub enum DataRepositoryError {
    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error("unable to create index: `{0}`")]
    IndexCreation(String),

    #[error(transparent)]
    RetrievalError(#[from] IndexError),
}

pub struct DataRepositoryManager {
    repository: Arc<Repository>,
    vector_index_manager: Arc<VectorIndexManager>,
    attribute_index_manager: Arc<AttributeIndexManager>,
    blob_storage: BlobStorageTS,
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
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_db(
        db: DbConn,
        vector_index_manager: Arc<VectorIndexManager>,
        blob_storage: BlobStorageTS,
    ) -> Self {
        let repository = Arc::new(Repository::new_with_db(db));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));
        Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
            blob_storage,
        }
    }

    #[tracing::instrument]
    pub async fn create_default_repository(&self, _server_config: &ServerConfig) -> Result<()> {
        let resp = self
            .repository
            .repository_by_name(DEFAULT_REPOSITORY_NAME)
            .await;
        if resp.is_err() {
            info!("creating default repository");
            let default_repo = DataRepository {
                name: DEFAULT_REPOSITORY_NAME.into(),
                extractor_bindings: vec![],
                data_connectors: vec![],
                metadata: HashMap::new(),
            };
            return self.create(&default_repo).await;
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_repositories(&self) -> Result<Vec<DataRepository>, DataRepositoryError> {
        self.repository
            .repositories()
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    #[tracing::instrument]
    async fn create_index(
        &self,
        extractor: &Extractor,
        repository: &str,
        extractor_binding: &ExtractorBinding,
    ) -> Result<Vec<String>> {
        let mut index_names = Vec::new();

        for (output_name, schema) in extractor.schemas.outputs.clone() {
            let index_name = format!("{}-{}", extractor_binding.name, output_name);
            info!(
                "adding index to extractor bindings repository: {}, extractor: {}, binding: {}, index: {}",
                repository, extractor_binding.extractor, extractor_binding.name, index_name
            );
            match schema {
                ExtractorOutputSchema::Embedding(schema) => {
                    self.vector_index_manager
                        .create_index(repository, &index_name, &extractor.name, schema)
                        .await
                        .map(|index_name| index_names.push(index_name.clone()))
                        .map_err(|e| DataRepositoryError::IndexCreation(e.to_string()))?;
                }
                ExtractorOutputSchema::Attributes { .. } => {
                    self.attribute_index_manager
                        .create_index(repository, &index_name, extractor.clone())
                        .await
                        .map(|index_name| index_names.push(index_name.clone()))
                        .map_err(|e| DataRepositoryError::IndexCreation(e.to_string()))?;
                }
            };
        }
        Ok(index_names)
    }

    #[tracing::instrument]
    pub async fn create(&self, repository: &DataRepository) -> Result<()> {
        info!("creating data repository: {}", repository.name);
        self.repository
            .upsert_repository(repository.clone())
            .await?;

        for extractor_binding in &repository.extractor_bindings {
            let _ = self
                .add_extractor_binding(&repository.name, extractor_binding)
                .await;
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get(&self, name: &str) -> Result<DataRepository, DataRepositoryError> {
        self.repository
            .repository_by_name(name)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn add_extractor_binding(
        &self,
        repository: &str,
        extractor_binding: &ExtractorBinding,
    ) -> Result<Vec<String>> {
        info!(
            "adding extractor bindings repository: {}, extractor: {}, binding: {}",
            repository, extractor_binding.extractor, extractor_binding.name,
        );
        let mut data_repository = self
            .repository
            .repository_by_name(repository)
            .await
            .unwrap();
        for ex in &data_repository.extractor_bindings {
            if ex.name == extractor_binding.name {
                return Err(anyhow!(
                    "binding with name {} already exists in repository: {}",
                    extractor_binding.name,
                    repository,
                ));
            }
        }
        let extractor = self
            .repository
            .extractor_by_name(&extractor_binding.extractor)
            .await?;
        let input_params_schema = JSONSchema::compile(&extractor.input_params).map_err(|e| {
            anyhow!(
                "unable to compile json schema for input params: {:?}, error: {:?}",
                &extractor.input_params,
                e
            )
        })?;
        let validation_result = input_params_schema.validate(&extractor_binding.input_params);
        if let Err(errors) = validation_result {
            let errors = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>();
            return Err(anyhow!(
                "unable to validate input params for extractor binding: {}, errors: {}",
                extractor_binding.name,
                errors.join(",")
            ));
        }
        let index_names = self.create_index(&extractor, repository, extractor_binding)
            .await?;
        data_repository
            .extractor_bindings
            .push(extractor_binding.clone());
        self.repository.upsert_repository(data_repository).await?;
        Ok(index_names)
    }

    #[tracing::instrument]
    pub async fn add_texts(&self, repo_name: &str, texts: Vec<ContentPayload>) -> Result<()> {
        let _ = self.repository.repository_by_name(repo_name).await?;
        self.repository.add_content(repo_name, texts).await
    }

    #[tracing::instrument]
    pub async fn list_indexes(&self, repository_name: &str) -> Result<Vec<Index>> {
        let indexes = self
            .repository
            .list_indexes(repository_name)
            .await
            .map_err(|e| anyhow!("unable to list indexes, error: {}", e.to_string()))?;
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
    pub async fn list_extractors(&self) -> Result<Vec<Extractor>, DataRepositoryError> {
        let extractors = self
            .repository
            .list_extractors()
            .await
            .map_err(DataRepositoryError::Persistence)?;
        Ok(extractors)
    }

    #[tracing::instrument]
    pub async fn add_events(
        &self,
        repository: &str,
        events: Vec<Event>,
    ) -> Result<(), DataRepositoryError> {
        self.repository
            .add_events(repository, events)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    #[tracing::instrument]
    pub async fn list_events(&self, repository: &str) -> Result<Vec<Event>, DataRepositoryError> {
        self.repository
            .list_events(repository)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    #[tracing::instrument]
    pub async fn upload_file(
        &self,
        repository: &str,
        name: &str,
        file: Bytes,
    ) -> Result<(), anyhow::Error> {
        // TODO - wrap the write to blob storage in a lambda and pass it to the
        // persistence layer so that we can mark the file upload as complete if
        // the blob storage write succeeds.
        let stored_file_path = self.blob_storage.put(name, file).await?;
        self.repository
            .add_content(
                repository,
                vec![ContentPayload::from_file(
                    repository,
                    name,
                    &stored_file_path,
                )],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::{
        blob_storage::BlobStorageBuilder,
        persistence::{DataConnector, Event, ExtractorBinding, SourceType},
        test_util,
        test_util::db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_repository() {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, ..) = test_util::db_utils::create_index_manager(db.clone()).await;
        let blob_storage =
            BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).unwrap();
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), index_manager, blob_storage);
        let mut meta = HashMap::new();
        meta.insert("foo".to_string(), json!(12));
        let repository = DataRepository {
            name: "test".to_string(),
            extractor_bindings: vec![ExtractorBinding::new(
                "test_extractor_binding",
                "test",
                DEFAULT_TEST_EXTRACTOR.to_string(),
                vec![],
                serde_json::json!({}),
            )],
            metadata: meta.clone(),
            data_connectors: vec![DataConnector {
                source: SourceType::GoogleContact {
                    metadata: Some("data_connector_meta".to_string()),
                },
            }],
        };
        repository_manager.create(&repository).await.unwrap();
        let repositories = repository_manager.list_repositories().await.unwrap();
        assert_eq!(repositories.len(), 1);
        assert_eq!(repositories[0].name, "test");
        assert_eq!(repositories[0].extractor_bindings.len(), 1);
        assert_eq!(repositories[0].data_connectors.len(), 1);
        assert_eq!(repositories[0].metadata, meta);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_events() {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, extractor_executor, coordinator) =
            test_util::db_utils::create_index_manager(db.clone()).await;

        let blob_storage =
            BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).unwrap();
        let repository_manager = Arc::new(DataRepositoryManager::new_with_db(
            db.clone(),
            index_manager.clone(),
            blob_storage,
        ));
        info!("creating repository");

        repository_manager
            .create(&test_util::db_utils::default_test_data_repository())
            .await
            .unwrap();

        let messages: Vec<Event> = vec![
            Event::new("hello world", None, HashMap::new()),
            Event::new("hello friend", None, HashMap::new()),
            Event::new("how are you", None, HashMap::new()),
        ];

        info!("adding messages to session");
        repository_manager
            .add_events(DEFAULT_TEST_REPOSITORY, messages.clone())
            .await
            .unwrap();

        let retrieve_result = repository_manager
            .list_events(DEFAULT_TEST_REPOSITORY)
            .await
            .unwrap();
        assert_eq!(retrieve_result.len(), 3);

        info!("manually syncing messages");
        coordinator.process_and_distribute_work().await.unwrap();
        let executor_id = extractor_executor.get_executor_info().id;
        let work_list = coordinator.get_work_for_worker(&executor_id).await.unwrap();

        extractor_executor.sync_repo_test(work_list).await.unwrap();

        //let search_results = repository_manager
        //    .search(DEFAULT_TEST_REPOSITORY, "memory_session_embeddings",
        // "hello", 2)    .await
        //    .unwrap();
        //assert_eq!(search_results.len(), 2);
    }
}
