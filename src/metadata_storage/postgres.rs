use std::{
    fmt,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use gluesql::core::error::Error::StorageMsg as GlueStorageError;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

use super::{
    sqlx::{row_to_extracted_metadata, row_to_metadata_scan_item},
    table_name,
    ExtractedMetadata,
    MetadataReader,
    MetadataScanStream,
    MetadataStorage,
};
use crate::utils::{timestamp_secs, PostgresIndexName};

pub struct PostgresIndexManager {
    pool: Pool<Postgres>,
    default_index_created: AtomicBool,
}

impl fmt::Debug for PostgresIndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttributeIndexManager").finish()
    }
}

impl PostgresIndexManager {
    pub fn new(conn_url: &str) -> Result<Arc<Self>> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(conn_url)?;
        Ok(Arc::new(Self {
            pool,
            default_index_created: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl MetadataStorage for PostgresIndexManager {
    async fn create_metadata_table(&self, namespace: &str) -> Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" (
            id TEXT PRIMARY KEY,
            namespace TEXT,
            extractor TEXT,
            extractor_policy TEXT,
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
    async fn drop_metadata_table(&self, namespace: &str) -> Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("DROP TABLE IF EXISTS \"{table_name}\";");
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    async fn add_metadata(&self, namespace: &str, metadata: ExtractedMetadata) -> Result<()> {
        if !self
            .default_index_created
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.create_metadata_table(namespace).await?;
            self.default_index_created
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("INSERT INTO \"{table_name}\" (id, namespace, extractor, extractor_policy, content_source, index_name, data, content_id, parent_content_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;");
        let _ = sqlx::query(&query)
            .bind(metadata.id)
            .bind(namespace)
            .bind(metadata.extractor_name)
            .bind(metadata.extraction_policy)
            .bind(metadata.content_source)
            .bind(table_name.to_string())
            .bind(metadata.metadata)
            .bind(metadata.content_id)
            .bind(metadata.parent_content_id)
            .bind(timestamp_secs() as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn remove_metadata(&self, namespace: &str, id: &str) -> Result<()> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("DELETE FROM \"{table_name}\" WHERE id = $1");
        let _ = sqlx::query(&query).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    async fn get_metadata_for_content(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!(
            "SELECT * FROM \"{index_table_name}\" WHERE namespace = $1 and content_id = $2"
        );
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

    async fn delete_metadata_for_content(&self, namespace: &str, content_id: &str) -> Result<()> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
        let query =
            format!("DELETE FROM \"{index_table_name}\" WHERE namespace = $1 and content_id = $2");

        sqlx::query(&query)
            .bind(namespace)
            .bind(content_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[async_trait(?Send)]
impl MetadataReader for PostgresIndexManager {
    async fn get_metadata_for_id(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<ExtractedMetadata>> {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!("SELECT * FROM \"{table_name}\" WHERE namespace = $2 and id = $3");
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
            FROM \"{table_name}\"
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
                    GlueStorageError(format!("error scanning metadata from postgres: {}", e))
                })?;

                row_to_metadata_scan_item(&row)
            });

        Ok(Box::pin(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_storage::test_metadata_storage;

    #[tokio::test]
    async fn test_postgres_metadata_storage() {
        let index_manager =
            PostgresIndexManager::new("postgres://postgres:postgres@localhost:5432/indexify")
                .unwrap();

        test_metadata_storage(index_manager).await;
    }
}
