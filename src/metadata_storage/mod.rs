use async_trait::async_trait;
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}, sync::Arc};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use self::{postgres::PostgresIndexManager, sqlite::SqliteIndexManager};
use crate::server_config::{MetadataStoreConfig, MetadataStoreKind};
pub mod postgres;
pub mod sqlite;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedMetadata {
    pub id: String,
    pub content_id: String,
    pub parent_content_id: String,
    pub metadata: serde_json::Value,
    pub extractor_name: String,
}

impl ExtractedMetadata {
    pub fn new(
        content_id: &str,
        parent_content_id: &str,
        metadata: serde_json::Value,
        extractor_name: &str,
        repository: &str,
    ) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        extractor_name.hash(&mut s);
        repository.hash(&mut s);
        metadata.to_string().hash(&mut s);
        let id = format!("{:x}", s.finish());
        Self {
            id,
            content_id: content_id.into(),
            parent_content_id: parent_content_id.into(),
            metadata,
            extractor_name: extractor_name.into(),
        }
    }
}

pub type MetadataStorageTS = Arc<dyn MetadataStorage + Sync + Send>;

#[async_trait]
pub trait MetadataStorage {
    async fn create_index(&self, index_name: &str, table_name: &str) -> Result<String>;

    async fn add_metadata(
        &self,
        repository: &str,
        index_name: &str,
        metadata: ExtractedMetadata,
    ) -> Result<()>;

    async fn get_metadata(
        &self,
        namespace: &str,
        index_table_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedMetadata>>;
}

pub fn from_config(config: &MetadataStoreConfig) -> Result<MetadataStorageTS> {
    match config.metadata_store {
        MetadataStoreKind::Postgres => Ok(PostgresIndexManager::new(&config.conn_url)?),
        MetadataStoreKind::Sqlite => Ok(SqliteIndexManager::new(&config.conn_url)?),
    }
}
