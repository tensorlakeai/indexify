use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use gluesql::core::store::DataRow;
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

use self::{postgres::PostgresIndexManager, sqlite::SqliteIndexManager};
use crate::server_config::{MetadataStoreConfig, MetadataStoreKind};
pub mod postgres;
pub mod query_engine;
pub mod sqlite;
mod sqlx;

fn table_name(namespace: &str) -> String {
    format!("metadata_{}", namespace)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractedMetadata {
    pub id: String,
    pub content_id: String,
    pub parent_content_id: String,
    pub content_source: String,
    pub metadata: serde_json::Value,
    pub extractor_name: String,
    pub extraction_policy: String,
}

impl ExtractedMetadata {
    pub fn new(
        content_id: &str,
        parent_content_id: &str,
        content_source: &str,
        metadata: serde_json::Value,
        extractor_name: &str,
        extraction_policy: &str,
    ) -> Self {
        Self {
            id: nanoid!(16),
            content_id: content_id.into(),
            parent_content_id: parent_content_id.into(),
            content_source: content_source.into(),
            metadata,
            extractor_name: extractor_name.into(),
            extraction_policy: extraction_policy.into(),
        }
    }
}

pub type MetadataStorageTS = Arc<dyn MetadataStorage + Sync + Send>;

pub type MetadataReaderTS = Arc<dyn MetadataReader + Sync + Send>;

#[async_trait]
pub trait MetadataStorage {
    async fn create_metadata_table(&self, namespace: &str) -> Result<()>;

    async fn add_metadata(&self, namespace: &str, metadata: ExtractedMetadata) -> Result<()>;

    async fn remove_metadata(&self, namespace: &str, id: &str) -> Result<()>;

    async fn get_metadata_for_content(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>>;

    async fn delete_metadata_for_content(&self, namespace: &str, content_id: &str) -> Result<()>;

    #[cfg(test)]
    async fn drop_metadata_table(&self, namespace: &str) -> Result<()>;
}

pub type MetadataScanStream<'a> = std::result::Result<
    Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = std::result::Result<
                        (gluesql::prelude::Key, DataRow),
                        gluesql::prelude::Error,
                    >,
                > + 'a,
        >,
    >,
    gluesql::prelude::Error,
>;

#[async_trait(?Send)]
pub trait MetadataReader {
    async fn get_metadata_for_id(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<ExtractedMetadata>>;

    fn get_metadata_scan_query(&self, namespace: &str) -> String;

    async fn scan_metadata<'a>(
        &self,
        query: &'a str,
        namespace: &str,
        content_source: &str,
    ) -> MetadataScanStream<'a>;
}

pub fn from_config(config: &MetadataStoreConfig) -> Result<MetadataStorageTS> {
    match config.metadata_store {
        MetadataStoreKind::Postgres => Ok(PostgresIndexManager::new(&config.conn_url)?),
        MetadataStoreKind::Sqlite => Ok(SqliteIndexManager::new(&config.conn_url)?),
    }
}

pub fn from_config_reader(config: &MetadataStoreConfig) -> Result<MetadataReaderTS> {
    match config.metadata_store {
        MetadataStoreKind::Postgres => Ok(PostgresIndexManager::new(&config.conn_url)?),
        MetadataStoreKind::Sqlite => Ok(SqliteIndexManager::new(&config.conn_url)?),
    }
}

#[cfg(test)]
async fn test_metadata_storage(index_manager: MetadataStorageTS) {
    let namespace = "test_namespace";
    index_manager
        .create_metadata_table(namespace)
        .await
        .unwrap();
    let metadata = ExtractedMetadata {
        id: "test_id".into(),
        content_id: "test_content_id".into(),
        parent_content_id: "test_parent_content_id".into(),
        content_source: "test_content_source".into(),
        metadata: serde_json::json!({"test": "test"}),
        extractor_name: "test_extractor".into(),
        extraction_policy: "test_extractor_policy".into(),
    };
    index_manager
        .add_metadata(namespace, metadata.clone())
        .await
        .unwrap();

    // Retrieve the metadata from the database
    let metadata_out = index_manager
        .get_metadata_for_content(namespace, "test_content_id")
        .await
        .unwrap();

    assert_eq!(metadata_out.len(), 1);
    assert_eq!(metadata_out[0], metadata);

    // Delete the metadata from the database
    index_manager
        .delete_metadata_for_content(namespace, "test_content_id")
        .await
        .unwrap();

    let metadata_out = index_manager
        .get_metadata_for_content(namespace, "test_content_id")
        .await
        .unwrap();

    assert_eq!(metadata_out.len(), 0);
}
