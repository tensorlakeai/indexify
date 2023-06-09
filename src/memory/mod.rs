mod utils;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utils::{get_messages_from_search_results, get_messages_from_texts, get_texts_from_messages};
use uuid::Uuid;

use crate::{
    index::{Index, IndexManager},
    text_splitters::TextSplitterKind,
    CreateIndexParams,
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
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    pub async fn new(index_manager: Arc<IndexManager>) -> Result<Self, MemoryError> {
        Ok(Self { index_manager })
    }

    async fn _get_index_name(&self, session_id: String) -> Result<String, MemoryError> {
        let index_name = self
            .index_manager
            .get_index_name_for_memory_session(session_id)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(index_name)
    }

    async fn _get_index(&self, session_id: &String) -> Result<Index, MemoryError> {
        let index_name = &self._get_index_name(session_id.into()).await?;
        self.index_manager
            .load(index_name.into())
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?
            .ok_or(MemoryError::IndexNotFound(index_name.into()))
    }

    pub async fn create_session_index(
        &self,
        session_id: Option<String>,
        vectordb_params: CreateIndexParams,
        embedding_model: String,
        text_splitter: TextSplitterKind,
        metadata: HashMap<String, String>,
    ) -> Result<String, MemoryError> {
        let session_id = session_id.unwrap_or(Uuid::new_v4().to_string());
        let index_name = vectordb_params.name.clone();
        self.index_manager
            .create_index_for_memory_session(
                &session_id,
                index_name,
                metadata,
                vectordb_params,
                embedding_model,
                text_splitter,
            )
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(session_id.to_string())
    }

    pub async fn add_messages(
        &self,
        session_id: &String,
        messages: Vec<Message>,
    ) -> Result<(), MemoryError> {
        let texts = get_texts_from_messages(session_id, messages);
        let index = self._get_index(session_id).await?;
        index
            .add_texts(texts)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        Ok(())
    }

    pub async fn retrieve_messages(&self, session_id: String) -> Result<Vec<Message>, MemoryError> {
        let index = self._get_index(&session_id).await?;
        let texts = index
            .get_texts()
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        let messages = get_messages_from_texts(texts);
        Ok(messages)
    }

    pub async fn search(
        &self,
        session_id: &String,
        query: String,
        k: u64,
    ) -> Result<Vec<Message>, MemoryError> {
        let index = self._get_index(session_id).await?;
        let results = index
            .search(query, k)
            .await
            .map_err(|e| MemoryError::InternalError(e.to_string()))?;
        let messages = get_messages_from_search_results(results);
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::super::entity::content::Entity as ContentEntity;
    use super::super::entity::index::Entity as IndexEntity;
    use super::super::entity::index_chunks::Entity as IndexChunkEntity;
    use super::super::entity::memory_sessions::Entity as MemorySessionEntity;
    use sea_orm::entity::prelude::*;
    use sea_orm::{
        sea_query::TableCreateStatement, Database, DatabaseConnection, DbBackend, DbConn, DbErr,
        Schema,
    };
    use serde_json::json;

    use super::*;
    use std::env;
    use std::sync::Arc;

    use crate::VectorDBTS;
    use crate::{
        qdrant::QdrantDb, CreateIndexParams, EmbeddingRouter, MetricKind, QdrantConfig,
        ServerConfig, VectorIndexConfig,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_search() {
        env::set_var("RUST_LOG", "debug");
        let session_id = &Uuid::new_v4().to_string();
        let index_name = &"test_memory_index".to_string();
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index(index_name.into()).await.unwrap();
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());

        let index_params = CreateIndexParams {
            name: index_name.into(),
            vector_dim: 384,
            metric: MetricKind::Cosine,
            unique_params: None,
        };
        let index_config = VectorIndexConfig {
            index_store: crate::IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig {
                addr: "http://localhost:6334".into(),
            }),
        };
        let db = create_db().await.unwrap();
        let index_manager = IndexManager::new_with_db(index_config, embedding_router, db).unwrap();

        let memory_manager = MemoryManager::new(Arc::new(index_manager)).await.unwrap();

        memory_manager
            .create_session_index(
                Some(session_id.into()),
                index_params,
                "all-minilm-l12-v2".into(),
                TextSplitterKind::Noop,
                HashMap::new(),
            )
            .await
            .unwrap();

        let messages: Vec<Message> = vec![
            Message {
                text: "hello world".into(),
                role: "human".into(),
                metadata: json!({}),
            },
            Message {
                text: "hello friend".into(),
                role: "AI".into(),
                metadata: json!({}),
            },
            Message {
                text: "how are you".into(),
                role: "human".into(),
                metadata: json!({}),
            },
        ];

        memory_manager
            .add_messages(&session_id, messages.clone())
            .await
            .unwrap();

        let retrieve_result = memory_manager
            .retrieve_messages(session_id.into())
            .await
            .unwrap();
        let search_result = memory_manager
            .search(session_id, "friend".into(), 1)
            .await
            .unwrap();

        let target_retrieve_result: Vec<Message> = vec![
            Message {
                text: "hello world".into(),
                role: "human".into(),
                metadata: json!({
                    "role": "human",
                    "session_id": session_id.to_string(),
                }),
            },
            Message {
                text: "hello friend".into(),
                role: "AI".into(),
                metadata: json!({
                    "role": "AI",
                    "session_id": session_id.to_string(),
                }),
            },
            Message {
                text: "how are you".into(),
                role: "human".into(),
                metadata: json!({
                    "role": "human",
                    "session_id": session_id.to_string(),
                }),
            },
        ];
        assert_eq!(retrieve_result, target_retrieve_result);
        assert_eq!(search_result.len(), 1);
        assert_eq!(search_result[0].text, "hello friend".to_string());
    }

    async fn create_db() -> Result<DatabaseConnection, DbErr> {
        let db = Database::connect("sqlite::memory:").await?;

        setup_schema(&db).await?;

        Ok(db)
    }

    async fn setup_schema(db: &DbConn) -> Result<(), DbErr> {
        // Setup Schema helper
        let schema = Schema::new(DbBackend::Sqlite);

        // Derive from Entity
        let stmt1: TableCreateStatement = schema.create_table_from_entity(IndexEntity);
        let stmt2: TableCreateStatement = schema.create_table_from_entity(ContentEntity);
        let stmt3: TableCreateStatement = schema.create_table_from_entity(IndexChunkEntity);
        let stm4: TableCreateStatement = schema.create_table_from_entity(MemorySessionEntity);

        // Execute create table statement
        db.execute(db.get_database_backend().build(&stmt1)).await?;
        db.execute(db.get_database_backend().build(&stmt2)).await?;
        db.execute(db.get_database_backend().build(&stmt3)).await?;
        db.execute(db.get_database_backend().build(&stm4)).await?;
        Ok(())
    }
}
