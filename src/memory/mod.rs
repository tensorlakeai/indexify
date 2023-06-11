mod utils;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utils::{get_messages_from_texts, get_texts_from_messages};
use uuid::Uuid;

use crate::{
    data_repository_manager::{DataRepositoryError, DataRepositoryManager},
    persistence::{ContentType, ExtractorConfig, ExtractorType},
    text_splitters::TextSplitterKind,
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
    text: String,
    role: String,
    metadata: HashMap<String, serde_json::Value>,
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
            repository: repository,
            default_embedding_model: default_embedding_model.into(),
        })
    }

    pub async fn create_session(
        &self,
        repository_id: &str,
        session_id: Option<String>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<String, MemoryError> {
        let session_id = session_id.unwrap_or(Uuid::new_v4().to_string());
        self.repository
            .create_memory_session(&session_id, repository_id, metadata)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;

        let mut repo = self.repository.get(repository_id).await?;
        repo.extractors.push(ExtractorConfig {
            name: session_id.clone(),
            content_type: ContentType::Memory,
            extractor_type: ExtractorType::Embedding {
                model: self.default_embedding_model.clone(),
                text_splitter: TextSplitterKind::Noop,
                distance: crate::IndexDistance::Cosine,
            },
        });
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

    use serde_json::json;

    use super::*;
    use std::env;
    use std::sync::Arc;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_search() {
        env::set_var("RUST_LOG", "debug");
        let session_id = &Uuid::new_v4().to_string();
        let repo = "default";
        let index_name = "default/default";
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, extractor_runner) =
            test_util::db_utils::create_index_manager(db.clone(), index_name).await;
        let repository_manager = DataRepositoryManager::new_with_db(db.clone(), index_manager);
        repository_manager
            .sync(&DataRepository::default())
            .await
            .unwrap();

        let memory_manager = MemoryManager::new(Arc::new(repository_manager), "all-minilm-l12-v2")
            .await
            .unwrap();

        memory_manager
            .create_session(repo, Some(session_id.into()), HashMap::new())
            .await
            .unwrap();

        let messages: Vec<Message> = vec![
            Message {
                text: "hello world".into(),
                role: "human".into(),
                metadata: HashMap::new(),
            },
            Message {
                text: "hello friend".into(),
                role: "AI".into(),
                metadata: HashMap::new(),
            },
            Message {
                text: "how are you".into(),
                role: "human".into(),
                metadata: HashMap::new(),
            },
        ];

        memory_manager
            .add_messages(repo, session_id, messages.clone())
            .await
            .unwrap();

        extractor_runner._sync_repo(repo).await.unwrap();

        let retrieve_result = memory_manager
            .retrieve_messages(repo, session_id.into())
            .await
            .unwrap();

        let target_retrieve_result: Vec<Message> = vec![
            Message {
                text: "hello world".into(),
                role: "human".into(),
                metadata: HashMap::from([
                    ("role".to_string(), json!("human")),
                    ("session_id".to_string(), json!(session_id)),
                ]),
            },
            Message {
                text: "hello friend".into(),
                role: "AI".into(),
                metadata: HashMap::from([
                    ("role".to_string(), json!("AI")),
                    ("session_id".to_string(), json!(session_id)),
                ]),
            },
            Message {
                text: "how are you".into(),
                role: "human".into(),
                metadata: HashMap::from([
                    ("role".to_string(), json!("human")),
                    ("session_id".to_string(), json!(session_id)),
                ]),
            },
        ];
        assert_eq!(retrieve_result, target_retrieve_result);
        let search_results = memory_manager
            .search(repo, session_id, "hello", 2)
            .await
            .unwrap();
        assert_eq!(search_results.len(), 2);
    }
}
