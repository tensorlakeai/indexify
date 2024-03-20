use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{anyhow, Ok, Result};
use async_trait::async_trait;
use futures::{
    stream::{self},
    StreamExt,
};
use gluesql::{
    core::{
        data::{HashMapJsonExt, Key, ValueError},
        error::Error::StorageMsg as GlueStorageError,
        store::DataRow,
    },
    json_storage::error::ResultExt,
};
use rusqlite::{params, Connection};
use tokio::sync::Mutex;

use super::{table_name, ExtractedMetadata, MetadataReader, MetadataScanStream, MetadataStorage};
use crate::utils::{timestamp_secs, PostgresIndexName};

pub struct SqliteIndexManager {
    conn: Arc<Mutex<Connection>>,
    default_table_created: AtomicBool,
}

impl SqliteIndexManager {
    pub fn new(conn_url: &str) -> Result<Arc<Self>> {
        let conn = if conn_url.starts_with("memory") {
            Connection::open_in_memory()
        } else {
            Connection::open(conn_url)
        };
        let conn = conn.map_err(|e| anyhow!("unable to open sqlite connection: {}", e))?;
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
            content_source TEXT,
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
        if !self
            .default_table_created
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.create_metadata_table(namespace).await?;
            self.default_table_created
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let query = format!("INSERT INTO {index_name} (id, namespace, extractor, extractor_policy_name, content_source, index_name, data, content_id, parent_content_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;");
        let conn = self.conn.lock().await;
        let _ = conn.execute(
            &query,
            params![
                metadata.id,
                namespace,
                metadata.extractor_name,
                metadata.extraction_policy,
                metadata.content_source,
                index_name.to_string(),
                metadata.metadata,
                metadata.content_id,
                metadata.parent_content_id,
                timestamp_secs() as i64,
            ],
        )?;
        Ok(())
    }

    async fn get_metadata_for_content(
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

#[async_trait(?Send)]
impl MetadataReader for SqliteIndexManager {
    async fn get_metadata_for_id(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<ExtractedMetadata>> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("SELECT * FROM {table_name} WHERE namespace = $1 and id = $2");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&query)?;
        let metadata = stmt
            .query_map(params![namespace, id], row_to_extracted_metadata)
            .map_err(|e| anyhow!("unable to query metadata from sqlite: {}", e))?;
        for metadata in metadata {
            let metadata = metadata.map_err(|e| anyhow!(e.to_string()))?;
            return Ok(Some(metadata));
        }
        Ok(None)
    }

    async fn scan_metadata(&self, namespace: &str, content_source: &str) -> MetadataScanStream {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("SELECT * FROM {table_name} WHERE namespace = $1 and content_source = $2");
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(&query).map_err(|e| {
            GlueStorageError(format!(
                "unable to execute query on sqlite: {}",
                e.to_string()
            ))
        })?;
        let metadata = stmt
            .query_map(params![namespace, content_source], |row| {
                row_to_extracted_metadata(row)
            })
            .map_err(|e| {
                GlueStorageError(format!("unable to query metadata from sqlite: {}", e))
            })?;
        let mut metadata_list = vec![];
        for m in metadata {
            let m = m.map_err(|e| GlueStorageError(e.to_string()))?;
            let mut out_rows: HashMap<String, gluesql::core::data::Value> = HashMap::new();
            out_rows.insert(
                "content_id".to_string(),
                gluesql::core::data::Value::Str(m.content_id.clone()),
            );
            let meta = match m.metadata.clone() {
                serde_json::Value::Object(json_map) => HashMap::try_from_json_map(json_map),
                _ => Err(ValueError::JsonObjectTypeRequired.into()),
            };
            let meta = meta
                .map_err(|e| gluesql::core::error::Error::StorageMsg(e.to_string()))
                .unwrap();
            out_rows.extend(meta);

            let key = gluesql::core::data::Key::Str(m.id.clone());
            metadata_list.push(std::result::Result::Ok((key, DataRow::Map(out_rows))));
        }
        let metadata_stream = stream::iter(metadata_list).boxed();
        std::result::Result::Ok(metadata_stream)
    }
}

fn row_to_extracted_metadata(
    row: &rusqlite::Row,
) -> std::result::Result<ExtractedMetadata, rusqlite::Error> {
    let id: String = row.get(0)?;
    let extractor_name: String = row.get(2)?;
    let extraction_policy: String = row.get(3)?;
    let content_source: String = row.get(5)?;
    let data: serde_json::Value = row.get(6)?;
    let content_id: String = row.get(7)?;
    let parent_content_id: String = row.get(8)?;
    std::result::Result::Ok(ExtractedMetadata {
        id,
        extractor_name,
        extraction_policy,
        content_source,
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
        index_manager
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
    }
}
