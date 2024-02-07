use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusqlite::Connection;
use tokio::sync::Mutex;

use super::{ExtractedMetadata, MetadataStorage};

pub struct SqliteIndexManager {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteIndexManager {
    pub fn new(conn_url: &str) -> Result<Arc<Self>> {
        let conn = Connection::open(conn_url)
            .map_err(|e| anyhow!("unable to open sqlite connection: {}", e))?;
        let conn = Arc::new(Mutex::new(conn));
        Ok(Arc::new(Self { conn }))
    }
}

#[async_trait]
impl MetadataStorage for SqliteIndexManager {
    async fn create_index(&self, index_name: &str, table_name: &str) -> Result<String> {
        Ok("".to_string())
    }

    async fn add_metadata(
        &self,
        repository: &str,
        index_name: &str,
        metadata: ExtractedMetadata,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_metadata(
        &self,
        namespace: &str,
        index_table_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedMetadata>> {
        Ok(vec![])
    }
}
