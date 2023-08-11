use sea_orm::DbConn;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tracing::info;

pub const DEFAULT_REPOSITORY_NAME: &str = "default";

use crate::{
    attribute_index::AttributeIndexManager,
    index::IndexError,
    persistence::{
        DataRepository, Event, ExtractedAttributes, ExtractorBinding, ExtractorConfig,
        ExtractorType, Repository, RepositoryError, Text,
    },
    vector_index::{ScoredText, VectorIndexManager},
    ServerConfig,
};

#[derive(Error, Debug)]
pub enum DataRepositoryError {
    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error("unable to create index: `{0}`")]
    IndexCreation(String),

    #[error(transparent)]
    RetrievalError(#[from] IndexError),

    #[error("operation not allowed: `{0}`")]
    NotAllowed(String),
}

pub struct DataRepositoryManager {
    repository: Arc<Repository>,
    vector_index_manager: Arc<VectorIndexManager>,
    attribute_index_manager: Arc<AttributeIndexManager>,
}

impl DataRepositoryManager {
    pub async fn new(
        repository: Arc<Repository>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_db(db: DbConn, vector_index_manager: Arc<VectorIndexManager>) -> Self {
        let repository = Arc::new(Repository::new_with_db(db));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));
        Self {
            repository,
            vector_index_manager,
            attribute_index_manager,
        }
    }

    pub async fn create_default_repository(
        &self,
        _server_config: &ServerConfig,
    ) -> Result<(), DataRepositoryError> {
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

    pub async fn list_repositories(&self) -> Result<Vec<DataRepository>, DataRepositoryError> {
        self.repository
            .repositories()
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn create(&self, repository: &DataRepository) -> Result<(), DataRepositoryError> {
        let _ = self
            .repository
            .upsert_repository(repository.clone())
            .await
            .map_err(DataRepositoryError::Persistence);
        for extractor_binding in &repository.extractor_bindings {
            let extractor = self
                .repository
                .extractor_by_name(&extractor_binding.extractor_name)
                .await?;
            if let ExtractorType::Embedding {
                dim: _,
                distance: _,
            } = extractor.extractor_type.clone()
            {
                self.vector_index_manager
                    .create_index(&repository.name, &extractor_binding.index_name, extractor)
                    .await
                    .map_err(|e| DataRepositoryError::IndexCreation(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub async fn get(&self, name: &str) -> Result<DataRepository, DataRepositoryError> {
        self.repository
            .repository_by_name(name)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn add_extractor_binding(
        &self,
        repository: &str,
        extractor: ExtractorBinding,
    ) -> Result<(), DataRepositoryError> {
        let mut data_repository = self
            .repository
            .repository_by_name(repository)
            .await
            .unwrap();
        for ex in &data_repository.extractor_bindings {
            if extractor.index_name == ex.index_name {
                return Err(DataRepositoryError::NotAllowed(format!(
                    "index with name `{}` already exists",
                    extractor.index_name,
                )));
            }
        }
        data_repository.extractor_bindings.push(extractor);
        self.create(&data_repository).await
    }

    pub async fn add_texts(
        &self,
        repo_name: &str,
        texts: Vec<Text>,
    ) -> Result<(), DataRepositoryError> {
        let _ = self.repository.repository_by_name(repo_name).await?;
        self.repository
            .add_content(repo_name, texts)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn search(
        &self,
        repository: &str,
        index_name: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<ScoredText>, DataRepositoryError> {
        self.vector_index_manager
            .search(repository, index_name, query, k as usize)
            .await
            .map_err(DataRepositoryError::RetrievalError)
    }

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

    pub async fn list_extractors(&self) -> Result<Vec<ExtractorConfig>, DataRepositoryError> {
        let extractors = self
            .repository
            .list_extractors()
            .await
            .map_err(DataRepositoryError::Persistence)?;
        Ok(extractors)
    }

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

    pub async fn list_events(&self, repository: &str) -> Result<Vec<Event>, DataRepositoryError> {
        self.repository
            .list_events(repository)
            .await
            .map_err(DataRepositoryError::Persistence)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::persistence::{DataConnector, Event, ExtractorBinding, SourceType};
    use crate::test_util;
    use crate::test_util::db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY};

    use serde_json::json;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_repository() {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, _, _) = test_util::db_utils::create_index_manager(db.clone()).await;
        let repository_manager = DataRepositoryManager::new_with_db(db.clone(), index_manager);
        let mut meta = HashMap::new();
        meta.insert("foo".to_string(), json!(12));
        let repository = DataRepository {
            name: "test".to_string(),
            extractor_bindings: vec![ExtractorBinding {
                extractor_name: DEFAULT_TEST_EXTRACTOR.to_string(),
                index_name: "default_embedder".to_string(),
                filters: vec![],
                input_params: serde_json::json!({}),
            }],
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
        let repository_manager = Arc::new(DataRepositoryManager::new_with_db(
            db.clone(),
            index_manager.clone(),
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
        //    .search(DEFAULT_TEST_REPOSITORY, "memory_session_embeddings", "hello", 2)
        //    .await
        //    .unwrap();
        //assert_eq!(search_results.len(), 2);
    }
}
