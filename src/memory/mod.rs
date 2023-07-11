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
    data_repository_manager::{DataRepositoryError, DataRepositoryManager},
    persistence::{ExtractorConfig, ExtractorType},
    text_splitters::TextSplitterKind,
    vectordbs::IndexDistance,
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

/// A struct that represents a manager for storing and retrieving from memory.
///
/// This struct provides methods for creating, storing, retrieving, and searching from memory sessions.
/// It persists the relationship between session ids and their corresponding indexes and persistent storage
/// implementations.
/// Each memory session will have a corresponding row in the memory_sessions table and index.
/// Each message has a corresponding point in vector DB and row in content table.
pub struct MemoryManager {
    repository: Arc<DataRepositoryManager>,
    default_embedding_model: String,
}

impl MemoryManager {
    pub async fn new(
        repository: Arc<DataRepositoryManager>,
        default_embedding_model: &str,
    ) -> Result<Self, MemoryError> {
        Ok(Self {
            repository,
            default_embedding_model: default_embedding_model.into(),
        })
    }

    pub async fn create_session(
        &self,
        repository_id: &str,
        session_id: Option<String>,
        extractor: Option<ExtractorConfig>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<String, MemoryError> {
        let session_id = session_id.unwrap_or(nanoid!());
        info!("creating memory session: {}", session_id);
        self.repository
            .create_memory_session(&session_id, repository_id, metadata)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;

        let mut repo = self.repository.get(repository_id).await?;
        let extractor = extractor.unwrap_or(ExtractorConfig {
            name: session_id.clone(),
            filter: crate::persistence::ExtractorFilter::MemorySession {
                session_id: session_id.clone(),
            },
            extractor_type: ExtractorType::Embedding {
                model: self.default_embedding_model.clone(),
                text_splitter: TextSplitterKind::Noop,
                distance: IndexDistance::Cosine,
            },
        });
        repo.extractors.push(extractor);
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
            .search_memory_session(repository, session_id, query, k)
            .await?;
        let messages = get_messages_from_texts(search_results);
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use crate::persistence::DataRepository;
    use crate::test_util;

    use super::*;

    use std::env;
    use std::sync::Arc;
    use tracing::info;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_search() {
        env::set_var("RUST_LOG", "debug");
        let session_id = &nanoid::nanoid!();
        let repo = "default";
        let index_name = "default/default";
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, extractor_runner) =
            test_util::db_utils::create_index_manager(db.clone(), index_name).await;
        let repository_manager = DataRepositoryManager::new_with_db(db.clone(), index_manager);
        info!("creating repository");
        repository_manager
            .sync(&DataRepository::default())
            .await
            .unwrap();

        let memory_manager = MemoryManager::new(Arc::new(repository_manager), "all-minilm-l12-v2")
            .await
            .unwrap();

        info!("creating session");
        memory_manager
            .create_session(repo, Some(session_id.into()), None, HashMap::new())
            .await
            .unwrap();

        let messages: Vec<Message> = vec![
            Message::new("hello world", "human", HashMap::new()),
            Message::new("hello friend", "ai", HashMap::new()),
            Message::new("how are you", "human", HashMap::new()),
        ];

        info!("adding messages to session");
        memory_manager
            .add_messages(repo, session_id, messages.clone())
            .await
            .unwrap();

        info!("manually syncing messages");
        extractor_runner.sync_repo(repo).await.unwrap();

        let retrieve_result = memory_manager
            .retrieve_messages(repo, session_id.into())
            .await
            .unwrap();
        assert_eq!(retrieve_result.len(), 3);

        let search_results = memory_manager
            .search(repo, session_id, "hello", 2)
            .await
            .unwrap();
        assert_eq!(search_results.len(), 2);
    }
}
