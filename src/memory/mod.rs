mod utils;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use nanoid::nanoid;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use tracing::info;
use utils::{get_messages_from_texts, get_texts_from_messages};

use crate::{
    data_repository_manager::{DataRepositoryError, DataRepositoryManager, DEFAULT_EXTRACTOR_NAME},
    persistence::ExtractorBinding,
};

/// An enumeration of possible errors that can occur while adding to or retrieving from memory.
#[derive(Error, Debug)]
pub enum MemoryError {
    /// An internal error that occurs during the operation.
    #[error("internal error: `{0}`")]
    InternalError(String),

    /// An error that occurs when corresponding index is not found.
    #[error("index `{0}` not found")]
    IndexNotFound(String),

    #[error(transparent)]
    RetrievalError(#[from] DataRepositoryError),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Message {
    pub id: String,
    pub text: String,
    pub role: String,
    pub unix_timestamp: u64,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Message {
    pub fn new(text: &str, role: &str, metadata: HashMap<String, serde_json::Value>) -> Self {
        let id = nanoid!();
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            id,
            text: text.into(),
            role: role.into(),
            unix_timestamp,
            metadata,
        }
    }
}

pub struct MemoryManager {
    repository: Arc<DataRepositoryManager>,
}

impl MemoryManager {
    pub async fn new(repository: Arc<DataRepositoryManager>) -> Result<Self, MemoryError> {
        Ok(Self { repository })
    }

    pub async fn create_session(
        &self,
        repository_id: &str,
        session_id: Option<String>,
        extractor: Option<ExtractorBinding>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<String, MemoryError> {
        let session_id = session_id.unwrap_or(nanoid!());
        info!("creating memory session: {}", session_id);
        self.repository
            .create_memory_session(&session_id, repository_id, metadata)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;

        let mut repo = self.repository.get(repository_id).await?;
        let extractor_binding = extractor.unwrap_or(ExtractorBinding {
            extractor_name: DEFAULT_EXTRACTOR_NAME.to_string(),
            index_name: session_id.clone(),
            filter: crate::persistence::ExtractorFilter::MemorySession {
                session_id: session_id.clone(),
            },
        });
        repo.extractor_bindings.push(extractor_binding);
        self.repository.sync(&repo).await?;
        Ok(session_id.to_string())
    }

    pub async fn add_messages(
        &self,
        repository: &str,
        session_id: &str,
        messages: Vec<Message>,
    ) -> Result<(), MemoryError> {
        let texts = get_texts_from_messages(session_id, messages);
        self.repository
            .add_texts(repository, texts, Some(session_id))
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(())
    }

    pub async fn retrieve_messages(
        &self,
        repository: &str,
        session_id: String,
    ) -> Result<Vec<Message>, MemoryError> {
        let texts = self
            .repository
            .memory_messages_for_session(repository, &session_id)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        let messages = get_messages_from_texts(texts);
        Ok(messages)
    }

    pub async fn search(
        &self,
        repository: &str,
        session_id: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<Message>, MemoryError> {
        let search_results = self
            .repository
            .search(repository, session_id, query, k)
            .await?;
        let messages = get_messages_from_texts(search_results);
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {

    use crate::test_util;
    use crate::test_util::db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY};

    use super::*;

    use std::env;
    use std::sync::Arc;
    use tracing::info;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_search() {
        env::set_var("RUST_LOG", "debug");
        let session_id = &nanoid::nanoid!();
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, extractor_executor, coordinator) =
            test_util::db_utils::create_index_manager(db.clone()).await;
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), index_manager.clone());
        info!("creating repository");

        repository_manager
            .sync(&test_util::db_utils::default_test_data_repository())
            .await
            .unwrap();

        let memory_manager = MemoryManager::new(Arc::new(repository_manager))
            .await
            .unwrap();

        info!("creating session");
        memory_manager
            .create_session(
                DEFAULT_TEST_REPOSITORY,
                Some(session_id.into()),
                Some(ExtractorBinding {
                    extractor_name: DEFAULT_TEST_EXTRACTOR.to_string(),
                    index_name: session_id.to_string(),
                    filter: crate::persistence::ExtractorFilter::MemorySession {
                        session_id: session_id.into(),
                    },
                }),
                HashMap::new(),
            )
            .await
            .unwrap();

        let messages: Vec<Message> = vec![
            Message::new("hello world", "human", HashMap::new()),
            Message::new("hello friend", "ai", HashMap::new()),
            Message::new("how are you", "human", HashMap::new()),
        ];

        info!("adding messages to session");
        memory_manager
            .add_messages(DEFAULT_TEST_REPOSITORY, session_id, messages.clone())
            .await
            .unwrap();

        let retrieve_result = memory_manager
            .retrieve_messages(DEFAULT_TEST_REPOSITORY, session_id.into())
            .await
            .unwrap();
        assert_eq!(retrieve_result.len(), 3);

        info!("manually syncing messages");
        coordinator.process_and_distribute_work().await.unwrap();
        let executor_id = extractor_executor.get_executor_info().id;
        let work_list = coordinator.get_work_for_worker(&executor_id).await.unwrap();

        extractor_executor.sync_repo_test(work_list).await.unwrap();

        let search_results = memory_manager
            .search(DEFAULT_TEST_REPOSITORY, session_id, "hello", 2)
            .await
            .unwrap();
        assert_eq!(search_results.len(), 2);
    }
}
