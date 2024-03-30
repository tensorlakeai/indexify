use std::{
    collections::BTreeMap,
    fmt,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use gluesql::core::{
    data::{Key, Value},
    error::Error::StorageMsg as GlueStorageError,
    store::DataRow,
};
use itertools::Itertools;
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    Pool,
    Postgres,
    Row,
};

use super::{table_name, ExtractedMetadata, MetadataReader, MetadataScanStream, MetadataStorage};
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

    async fn scan_metadata(&self, namespace: &str, content_source: &str) -> MetadataScanStream {
        let table_name = PostgresIndexName::new(&table_name(namespace));
        let query = format!(
            "
            SELECT content_id, data
            FROM \"{table_name}\"
            WHERE namespace = $1 AND content_source = $2
        "
        );
        let rows = sqlx::query(&query)
            .bind(namespace)
            .bind(content_source)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                GlueStorageError(format!("unable to query metadata from postgres: {}", e))
            })?
            .into_iter()
            .map(|row: PgRow| {
                let content_id: String = row.get(0);
                let mut out_rows: Vec<Value> = Vec::new();
                out_rows.push(Value::Str(content_id.clone()));

                let data: serde_json::Value = row.get(1);
                let data = match data {
                    serde_json::Value::Object(json_map) => json_map
                        .into_iter()
                        .map(|(key, value)| {
                            let value = Value::try_from(value)?;

                            Ok((key, value))
                        })
                        .collect::<Result<BTreeMap<String, Value>>>()
                        .map_err(|e| {
                            GlueStorageError(format!("invalid metadata from postgres: {}", e))
                        })?,
                    _ => return Err(GlueStorageError("expected JSON object".to_string())),
                };
                out_rows.extend(data.values().cloned().collect_vec());

                Ok((Key::Str(content_id), DataRow::Vec(out_rows)))
            });

        Ok(Box::pin(stream::iter(rows)))
    }
}

fn row_to_extracted_metadata(row: &PgRow) -> ExtractedMetadata {
    let id: String = row.get(0);
    let extractor: String = row.get(2);
    let extraction_policy: String = row.get(3);
    let content_source: String = row.get(4);
    let data: serde_json::Value = row.get(6);
    let content_id: String = row.get(7);
    let parent_content_id: String = row.get(8);
    ExtractedMetadata {
        id,
        content_id,
        parent_content_id,
        content_source,
        metadata: data,
        extractor_name: extractor,
        extraction_policy,
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
