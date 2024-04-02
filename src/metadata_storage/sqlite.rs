use std::sync::{atomic::AtomicBool, Arc};

use async_trait::async_trait;
use futures::StreamExt;
use gluesql::core::error::Error::StorageMsg as GlueStorageError;
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};

use super::{
    sqlx::{row_to_extracted_metadata, row_to_metadata_scan_item},
    table_name,
    ExtractedMetadata,
    MetadataReader,
    MetadataScanStream,
    MetadataStorage,
};
use crate::utils::{timestamp_secs, PostgresIndexName};

pub struct SqliteIndexManager {
    pool: Pool<Sqlite>,
    default_table_created: AtomicBool,
}

impl SqliteIndexManager {
    pub fn new(conn_url: &str) -> anyhow::Result<Arc<Self>> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_lazy(conn_url)?;
        Ok(Arc::new(Self {
            pool,
            default_table_created: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl MetadataStorage for SqliteIndexManager {
    async fn create_metadata_table(&self, namespace: &str) -> anyhow::Result<()> {
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
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    #[cfg(test)]
    async fn drop_metadata_table(&self, namespace: &str) -> anyhow::Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("DROP TABLE IF EXISTS {table_name};");
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    async fn add_metadata(
        &self,
        namespace: &str,
        metadata: ExtractedMetadata,
    ) -> anyhow::Result<()> {
        let index_name = PostgresIndexName::new(&table_name(namespace));
        if !self
            .default_table_created
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.create_metadata_table(namespace).await?;
            self.default_table_created
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let query = format!(
            "
            INSERT INTO {index_name} (
                id, namespace, extractor, extractor_policy_name,
                content_source, index_name, data, content_id,
                parent_content_id, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
        "
        );
        let _ = sqlx::query(&query)
            .bind(metadata.id)
            .bind(namespace)
            .bind(metadata.extractor_name)
            .bind(metadata.extraction_policy)
            .bind(metadata.content_source)
            .bind(index_name.to_string())
            .bind(metadata.metadata)
            .bind(metadata.content_id)
            .bind(metadata.parent_content_id)
            .bind(timestamp_secs() as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove_metadata(&self, namespace: &str, id: &str) -> anyhow::Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("DELETE FROM {table_name} WHERE id = $1");
        let _ = sqlx::query(&query).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    async fn get_metadata_for_content(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> anyhow::Result<Vec<ExtractedMetadata>> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("SELECT * FROM {index_table_name} WHERE namespace = $1 and content_id = $2");

        let extracted_attributes = sqlx::query(&query)
            .bind(namespace)
            .bind(content_id)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(row_to_extracted_metadata)
            .collect();

        Ok(extracted_attributes)
    }

    async fn delete_metadata_for_content(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> anyhow::Result<()> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("DELETE FROM {index_table_name} WHERE namespace = $1 and content_id = $2");

        sqlx::query(&query)
            .bind(namespace)
            .bind(content_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl MetadataReader for SqliteIndexManager {
    async fn get_metadata_for_id(
        &self,
        namespace: &str,
        id: &str,
    ) -> anyhow::Result<Option<ExtractedMetadata>> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("SELECT * FROM {table_name} WHERE namespace = $2 and id = $3");
        let metadata = sqlx::query(&query)
            .bind(namespace)
            .bind(id)
            .bind(table_name.to_string())
            .fetch_all(&self.pool)
            .await?
            .first()
            .map(row_to_extracted_metadata);

        Ok(metadata)
    }

    fn get_metadata_scan_query(&self, namespace: &str) -> String {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!(
            "
            SELECT content_id, data
            FROM {table_name}
            WHERE namespace = $1 AND content_source = $2
        "
        );

        query
    }

    async fn scan_metadata<'a>(
        &self,
        query: &'a str,
        namespace: &str,
        content_source: &str,
    ) -> MetadataScanStream<'a> {
        let rows = sqlx::query(query)
            .bind(namespace.to_string())
            .bind(content_source.to_string())
            .fetch(&self.pool)
            .then(|row| async move {
                let row = row.map_err(|e| {
                    GlueStorageError(format!("error scanning metadata from sqlite: {}", e))
                })?;

                row_to_metadata_scan_item(&row)
            });

        Ok(Box::pin(rows))
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Row;

    use super::*;
    use crate::{metadata_storage::test_metadata_storage, utils::PostgresIndexName};

    #[tokio::test]
    async fn test_create_index() {
        let index_manager = SqliteIndexManager::new("sqlite::memory:").unwrap();
        let namespace = "test_namespace";
        index_manager
            .create_metadata_table(namespace)
            .await
            .unwrap();
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';");
        let table_name_out: String = sqlx::query(&query)
            .fetch_one(&index_manager.pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(table_name_out, "metadata_test_namespace".to_string());
    }

    #[tokio::test]
    async fn test_sqlite_metadata_storage() {
        let index_manager = SqliteIndexManager::new("sqlite::memory:").unwrap();
        test_metadata_storage(index_manager).await;
    }
}
