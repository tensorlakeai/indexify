use sea_orm::DbConn;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;
use tracing::log::info;

use crate::{
    index::{CreateIndexArgs, IndexError, IndexManager},
    persistence::{
        ContentType, DataRepository, ExtractorConfig, ExtractorType, Repository, RepositoryError,
        Text,
    },
    text_splitters::TextSplitterKind,
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
        db_url: &str,
        index_manager: Arc<IndexManager>,
    ) -> Result<Self, RepositoryError> {
        let repository = Arc::new(Repository::new(db_url).await?);
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
        let resp = self.repository.repository_by_name("default").await;
        if resp.is_err() {
            info!("creating default repository");
            let default_repo = DataRepository {
                name: "default".into(),
                extractors: vec![ExtractorConfig {
                    name: "default".into(),
                    content_type: ContentType::Text,
                    extractor_type: ExtractorType::Embedding {
                        model: server_config.default_model().model_kind.to_string(),
                        text_splitter: TextSplitterKind::Noop,
                        distance: crate::IndexDistance::Cosine,
                    },
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
        for extractor in &repository.extractors {
            if let ExtractorType::Embedding {
                model,
                text_splitter,
                distance,
            } = extractor.extractor_type.clone()
            {
                let index_name = format!("{}/{}", repository.name, extractor.name);
                self.index_manager
                    .create_index(
                        CreateIndexArgs {
                            name: index_name,
                            distance,
                        },
                        &repository.name,
                        model,
                        text_splitter,
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
        extractor: ExtractorConfig,
    ) -> Result<(), DataRepositoryError> {
        let mut data_repository = self
            .repository
            .repository_by_name(repository)
            .await
            .unwrap();
        for ex in &data_repository.extractors {
            if extractor.name == ex.name {
                return Err(DataRepositoryError::NotAllowed(format!(
                    "extractor with name `{}` already exists",
                    extractor.name
                )));
            }
        }
        data_repository.extractors.push(extractor);
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
            .add_text_to_repo(repo_name, texts, memory_session)
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
            .get_texts_for_memory_session(repository, id)
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn search(
        &self,
        index_name: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<Text>, DataRepositoryError> {
        let index = self.index_manager.load(index_name).await?;
        index
            .search(query, k)
            .await
            .map_err(DataRepositoryError::RetrievalError)
    }

    pub async fn search_memory_session(
        &self,
        repository: &str,
        session_id: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<Text>, DataRepositoryError> {
        let index = format!("{}/{}", repository, session_id);
        self.search(&index, query, k).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::persistence::{DataConnector, ExtractorConfig, ExtractorType, SourceType};
    use crate::text_splitters::TextSplitterKind;
    use crate::{test_util, IndexDistance};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_repository() {
        let db = test_util::db_utils::create_db().await.unwrap();
        let index_name = "hello";
        let (index_manager, _) =
            test_util::db_utils::create_index_manager(db.clone(), "test/hello").await;
        let repository_manager = DataRepositoryManager::new_with_db(db.clone(), index_manager);
        let mut meta = HashMap::new();
        meta.insert("foo".to_string(), json!(12));
        let repository = DataRepository {
            name: "test".to_string(),
            extractors: vec![ExtractorConfig {
                name: index_name.into(),
                content_type: ContentType::Text,
                extractor_type: ExtractorType::Embedding {
                    model: "all-minilm-l12-v2".into(),
                    text_splitter: TextSplitterKind::Noop,
                    distance: IndexDistance::Cosine,
                },
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
        assert_eq!(repositories[0].extractors.len(), 1);
        assert_eq!(repositories[0].data_connectors.len(), 1);
        assert_eq!(repositories[0].metadata, meta);
    }
}
