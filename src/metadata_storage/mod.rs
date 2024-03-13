use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use self::{postgres::PostgresIndexManager, sqlite::SqliteIndexManager};
use crate::server_config::{MetadataStoreConfig, MetadataStoreKind};
pub mod postgres;
pub mod sqlite;

fn table_name(namespace: &str) -> String {
    format!("metadata_{}", namespace)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractedMetadata {
    pub id: String,
    pub content_id: String,
    pub parent_content_id: String,
    pub metadata: serde_json::Value,
    pub extractor_name: String,
    pub extraction_policy: String,
}

impl ExtractedMetadata {
    pub fn new(
        content_id: &str,
        parent_content_id: &str,
        metadata: serde_json::Value,
        extractor_name: &str,
        extraction_policy: &str,
        namespace: &str,
    ) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        extractor_name.hash(&mut s);
        namespace.hash(&mut s);
        metadata.to_string().hash(&mut s);
        extraction_policy.hash(&mut s);
        let id = format!("{:x}", s.finish());
        Self {
            id,
            content_id: content_id.into(),
            parent_content_id: parent_content_id.into(),
            metadata,
            extractor_name: extractor_name.into(),
            extraction_policy: extraction_policy.into(),
        }
    }
}

pub type MetadataStorageTS = Arc<dyn MetadataStorage + Sync + Send>;

#[async_trait]
pub trait MetadataStorage {
    async fn create_metadata_table(&self, namespace: &str) -> Result<()>;

    async fn add_metadata(&self, namespace: &str, metadata: ExtractedMetadata) -> Result<()>;

    async fn get_metadata(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>>;
}

pub fn from_config(config: &MetadataStoreConfig) -> Result<MetadataStorageTS> {
    match config.metadata_store {
        MetadataStoreKind::Postgres => Ok(PostgresIndexManager::new(&config.conn_url)?),
        MetadataStoreKind::Sqlite => Ok(SqliteIndexManager::new(&config.conn_url)?),
    }
}
