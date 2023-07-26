use sea_orm::DbConn;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tracing::info;

pub const DEFAULT_REPOSITORY_NAME: &str = "default";
pub const DEFAULT_EXTRACTOR_NAME: &str = "default_embedder";
pub const DEFAULT_INDEX_NAME: &str = "default_index";

use crate::{
    index::{CreateIndexArgs, IndexError, IndexManager},
    persistence::{
        ContentType, DataRepository, ExtractorBinding, ExtractorConfig, ExtractorFilter,
        ExtractorType, Repository, RepositoryError, Text,
    },
    text_splitters::TextSplitterKind,
    vectordbs::IndexDistance,
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
    index_manager: Arc<IndexManager>,
}

impl DataRepositoryManager {
    pub async fn new(
        repository: Arc<Repository>,
        index_manager: Arc<IndexManager>,
    ) -> Result<Self, RepositoryError> {
        Ok(Self {
            repository,
            index_manager,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_db(db: DbConn, index_manager: Arc<IndexManager>) -> Self {
        let repository = Arc::new(Repository::new_with_db(db));
        Self {
            repository,
            index_manager,
        }
    }

    pub async fn create_default_repository(
        &self,
        server_config: &ServerConfig,
    ) -> Result<(), DataRepositoryError> {
        let default_extractor = ExtractorConfig {
            name: DEFAULT_EXTRACTOR_NAME.into(),
            extractor_type: ExtractorType::Embedding {
                model: server_config.default_model().model_kind.to_string(),
                distance: IndexDistance::Cosine,
            },
        };
        self.repository
            .record_extractors(vec![default_extractor])
            .await?;
        let resp = self
            .repository
            .repository_by_name(DEFAULT_REPOSITORY_NAME)
            .await;
        if resp.is_err() {
            info!("creating default repository");
            let default_repo = DataRepository {
                name: DEFAULT_REPOSITORY_NAME.into(),
                extractor_bindings: vec![ExtractorBinding {
                    name: DEFAULT_EXTRACTOR_NAME.into(),
                    index_name: DEFAULT_INDEX_NAME.into(),
                    filter: ExtractorFilter::ContentType {
                        content_type: ContentType::Text,
                    },
                    text_splitter: TextSplitterKind::Noop,
                }],
                data_connectors: vec![],
                metadata: HashMap::new(),
            };
            return self.sync(&default_repo).await;
        }
        Ok(())
    }

    pub async fn list_repositories(&self) -> Result<Vec<DataRepository>, DataRepositoryError> {
        self.repository
            .repositories()
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn sync(&self, repository: &DataRepository) -> Result<(), DataRepositoryError> {
        let _ = self
            .repository
            .upsert_repository(repository.clone())
            .await
            .map_err(DataRepositoryError::Persistence);
        for extractor_binding in &repository.extractor_bindings {
            let extractor = self
                .repository
                .extractor_by_name(&extractor_binding.name)
                .await?;
            if let ExtractorType::Embedding { model, distance } = extractor.extractor_type.clone() {
                self.index_manager
                    .create_index(
                        &extractor_binding.name,
                        CreateIndexArgs {
                            name: extractor_binding.index_name.clone(),
                            distance,
                        },
                        &repository.name,
                        model,
                        extractor_binding.text_splitter.clone(),
                    )
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

    pub async fn add_extractor(
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
            if extractor.name == ex.name {
                return Err(DataRepositoryError::NotAllowed(format!(
                    "extractor with name `{}` already exists",
                    extractor.name
                )));
            }
        }
        data_repository.extractor_bindings.push(extractor);
        self.sync(&data_repository).await
    }

    pub async fn add_texts(
        &self,
        repo_name: &str,
        texts: Vec<Text>,
        memory_session: Option<&str>,
    ) -> Result<(), DataRepositoryError> {
        let _ = self.repository.repository_by_name(repo_name).await?;
        self.repository
            .add_content(repo_name, texts, memory_session)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn create_memory_session(
        &self,
        session_id: &str,
        repository_id: &str,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<(), DataRepositoryError> {
        let _ = self.repository.repository_by_name(repository_id).await?;
        self.repository
            .create_memory_session(session_id, repository_id, metadata)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn memory_messages_for_session(
        &self,
        repository: &str,
        id: &str,
    ) -> Result<Vec<Text>, DataRepositoryError> {
        self.repository
            .retrieve_messages_from_memory(repository, id)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn search(
        &self,
        repository: &str,
        index_name: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<Text>, DataRepositoryError> {
        let index = self.index_manager.load(repository, index_name).await?;
        index
            .search(query, k)
            .await
            .map_err(DataRepositoryError::RetrievalError)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::persistence::{ContentType, DataConnector, SourceType};
    use crate::test_util;
    use crate::test_util::db_utils::DEFAULT_TEST_EXTRACTOR;
    use crate::text_splitters::TextSplitterKind;

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
                name: DEFAULT_TEST_EXTRACTOR.to_string(),
                index_name: DEFAULT_EXTRACTOR_NAME.to_string(),
                filter: ExtractorFilter::ContentType {
                    content_type: ContentType::Text,
                },
                text_splitter: TextSplitterKind::Noop,
            }],
            metadata: meta.clone(),
            data_connectors: vec![DataConnector {
                source: SourceType::GoogleContact {
                    metadata: Some("data_connector_meta".to_string()),
                },
            }],
        };
        repository_manager.sync(&repository).await.unwrap();
        let repositories = repository_manager.list_repositories().await.unwrap();
        assert_eq!(repositories.len(), 1);
        assert_eq!(repositories[0].name, "test");
        assert_eq!(repositories[0].extractor_bindings.len(), 1);
        assert_eq!(repositories[0].data_connectors.len(), 1);
        assert_eq!(repositories[0].metadata, meta);
    }
}
