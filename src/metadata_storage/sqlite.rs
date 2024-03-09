use std::sync::{atomic::AtomicBool, Arc};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rusqlite::{params, Connection};
use tokio::sync::Mutex;

use super::{table_name, ExtractedMetadata, MetadataStorage};
use crate::utils::{timestamp_secs, PostgresIndexName};

pub struct SqliteIndexManager {
    conn: Arc<Mutex<Connection>>,
    default_table_created: AtomicBool,
}

impl SqliteIndexManager {
    pub fn new(conn_url: &str) -> Result<Arc<Self>> {
        let conn = Connection::open(conn_url)
            .map_err(|e| anyhow!("unable to open sqlite connection: {}", e))?;
        let conn = Arc::new(Mutex::new(conn));
        Ok(Arc::new(Self {
            conn,
            default_table_created: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl MetadataStorage for SqliteIndexManager {
    async fn create_metadata_table(&self, namespace: &str) -> Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT PRIMARY KEY,
            namespace TEXT,
            extractor TEXT,
            extractor_policy_name TEXT,
            index_name TEXT,
            data JSONB,
            content_id TEXT,
            parent_content_id TEXT,
            created_at BIGINT
        );"
        );
        let conn = self.conn.lock().await;
        let _ = conn.execute(&query, params![])?;
        Ok(())
    }

    async fn add_metadata(&self, namespace: &str, metadata: ExtractedMetadata) -> Result<()> {
        let index_name = PostgresIndexName::new(&table_name(namespace));
        if self
            .default_table_created
            .load(std::sync::atomic::Ordering::Relaxed) ==
            false
        {
            self.create_metadata_table(namespace).await?;
            self.default_table_created
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let query = format!("INSERT INTO {index_name} (id, namespace, extractor, extractor_policy_name, index_name, data, content_id, parent_content_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;");
        let conn = self.conn.lock().await;
        let _ = conn.execute(
            &query,
            params![
                metadata.id,
                namespace,
                metadata.extractor_name,
                metadata.extractor_policy_name,
                index_name.to_string(),
                metadata.metadata,
                metadata.content_id,
                metadata.parent_content_id,
                timestamp_secs() as i64,
            ],
        )?;
        Ok(())
    }

    async fn get_metadata(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
        let conn = self.conn.lock().await;
        let query =
            format!("SELECT * FROM {index_table_name} WHERE namespace = $1 and content_id = $2");
        let mut stmt = conn.prepare(&query)?;
        let metadata_iter = stmt
            .query_map(params![namespace, content_id], |row| {
                row_to_extracted_metadata(row)
            })
            .map_err(|e| anyhow!("unable to query metadata from sqlite: {}", e))?;
        let mut extracted_attributes = Vec::new();
        for metadata in metadata_iter {
            extracted_attributes.push(metadata?);
        }
        Ok(extracted_attributes)
    }
}

fn row_to_extracted_metadata(
    row: &rusqlite::Row,
) -> std::result::Result<ExtractedMetadata, rusqlite::Error> {
    let id: String = row.get(0)?;
    let extractor: String = row.get(2)?;
    let extractor_policy_name: String = row.get(3)?;
    let data: serde_json::Value = row.get(5)?;
    let content_id: String = row.get(6)?;
    let parent_content_id: String = row.get(7)?;
    Ok(ExtractedMetadata {
        id,
        extractor_name: extractor,
        extractor_policy_name,
        metadata: data,
        content_id,
        parent_content_id,
    })
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use super::*;
    use crate::utils::PostgresIndexName;

    #[tokio::test]
    async fn test_create_index() {
        let conn = Connection::open_in_memory().unwrap();
        let conn = Arc::new(Mutex::new(conn));
        let index_manager = SqliteIndexManager {
            conn,
            default_table_created: AtomicBool::new(false),
        };
        let namespace = "test_namespace";
        let _ = index_manager
            .create_metadata_table(namespace)
            .await
            .unwrap();
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';");
        let conn = index_manager.conn.lock().await;
        let mut stmt = conn.prepare(&query).unwrap();
        let table_name_out: String = stmt.query_row(params![], |row| row.get(0)).unwrap();
        assert_eq!(table_name_out, "metadata_test_namespace".to_string());
    }

    #[tokio::test]
    async fn test_add_metadata() {
        let conn = Connection::open_in_memory().unwrap();
        let conn = Arc::new(Mutex::new(conn));
        let index_manager = SqliteIndexManager {
            conn,
            default_table_created: AtomicBool::new(false),
        };
        let namespace = "test_namespace";
        let _ = index_manager
            .create_metadata_table(namespace)
            .await
            .unwrap();
        let metadata = ExtractedMetadata {
            id: "test_id".into(),
            content_id: "test_content_id".into(),
            parent_content_id: "test_parent_content_id".into(),
            metadata: serde_json::json!({"test": "test"}),
            extractor_name: "test_extractor".into(),
            extractor_policy_name: "test_extractor_policy".into(),
        };
        index_manager
            .add_metadata(namespace, metadata.clone())
            .await
            .unwrap();

        // Retrieve the metadata from the database
        let metadata_out = index_manager
            .get_metadata(namespace, "test_content_id")
            .await
            .unwrap();

        assert_eq!(metadata_out.len(), 1);
        assert_eq!(metadata_out[0], metadata);
    }
}
