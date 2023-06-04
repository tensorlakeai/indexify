mod utils;
use std::sync::Arc;

use anyhow::Result;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utils::{get_messages_from_search_results, get_messages_from_texts, get_texts_from_messages};
use uuid::Uuid;

use crate::{index::IndexManager, text_splitters::TextSplitterKind, CreateIndexParams, MetricKind};

/// An enumeration of possible errors that can occur while adding to or retrieving from memory.
#[derive(Error, Debug)]
pub enum MemoryError {
    /// An internal error that occurs during the operation.
    #[error("internal error: `{0}`")]
    InternalError(String),

    /// An error that occurs when requested session is not found.
    #[error("session `{0}` not found")]
    SessionNotFound(Uuid),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    text: String,
    role: String,
    metadata: serde_json::Value,
}

/// A struct that represents a manager for storing and retrieving from memory.
///
/// This struct provides methods for creating, storing, retrieving, and searching from memory sessions.
/// It persists the relationship between session ids and their corresponding indexes and persistent storage
/// implementations.
/// Each memory session will have a corresponding row in the memory_sessions table and index.
/// Each message has a corresponding point in vector DB and row in content table.
pub struct MemoryManager {
    index_manager: Arc<IndexManager>,
}

impl MemoryManager {
    pub async fn new(index_manager: Arc<IndexManager>) -> Result<Option<Self>, MemoryError> {
        // TODO: Create memory_sessions DB table to persist session_id and index_name
        Ok(Some(Self { index_manager }))
    }

    fn _get_index_name(&self, session_id: Uuid) -> Result<String, MemoryError> {
        // TODO: Create better default index name without exposing session_id
        // TODO: Retrieve index_name from memory_sessions DB table
        return Ok(format!("{}", session_id));
    }

    pub async fn create_session_index(
        &self,
        session_id: Option<Uuid>,
    ) -> Result<Uuid, MemoryError> {
        let session_id = session_id.unwrap_or(Uuid::new_v4());
        let index_name = self._get_index_name(session_id)?;
        // TODO: Persist session_id and index_name to memory_sessions DB table
        self.index_manager
            .create_index(
                CreateIndexParams {
                    name: index_name.into(),
                    vector_dim: 384,
                    metric: MetricKind::Cosine,
                    unique_params: None,
                },
                "all-minilm-l12-v2".into(),
                TextSplitterKind::Noop,
            )
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(session_id)
    }

    pub async fn add_messages(
        &self,
        session_id: Uuid,
        messages: Vec<Message>,
    ) -> Result<(), MemoryError> {
        let index_name = self._get_index_name(session_id)?;
        let texts = get_texts_from_messages(session_id, messages);
        let index = self
            .index_manager
            .load(index_name.into())
            .await
            .unwrap()
            .unwrap();
        index
            .add_texts(texts)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(())
    }

    pub async fn retrieve_messages(&self, session_id: Uuid) -> Result<Vec<Message>, MemoryError> {
        let index_name = self._get_index_name(session_id)?;
        let index = self
            .index_manager
            .load(index_name.into())
            .await
            .unwrap()
            .unwrap();
        let texts = index
            .get_texts()
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        let messages = get_messages_from_texts(texts);
        Ok(messages)
    }

    pub async fn search(
        &self,
        session_id: Uuid,
        query: String,
        k: u64,
    ) -> Result<Vec<Message>, MemoryError> {
        let index_name = self._get_index_name(session_id)?;
        let index = self
            .index_manager
            .load(index_name.into())
            .await
            .unwrap()
            .unwrap();
        let results = index
            .search(query, k)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        let messages = get_messages_from_search_results(results);
        Ok(messages)
    }
}
