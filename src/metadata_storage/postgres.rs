use std::{fmt, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use super::{ExtractedMetadata, MetadataStorage};
use crate::utils::{timestamp_secs, PostgresIndexName};

pub struct PostgresIndexManager {
    pool: Pool<Postgres>,
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
        Ok(Arc::new(Self { pool }))
    }
}

#[async_trait]
impl MetadataStorage for PostgresIndexManager {
    async fn create_index(&self, index_name: &str, table_name: &str) -> Result<String> {
        let table_name = PostgresIndexName::new(table_name);
        let query = format!(
            "CREATE TABLE IF NOT EXISTS \"{table_name}\" (
            id TEXT PRIMARY KEY,
            namespace TEXT,
            extractor TEXT,
            index_name TEXT,
            data JSONB,
            content_id TEXT,
            parent_content_id TEXT,
            created_at BIGINT
        );"
        );
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(index_name.to_string())
    }

    async fn add_metadata(
        &self,
        namespace: &str,
        index_name: &str,
        metadata: ExtractedMetadata,
    ) -> Result<()> {
        let index_name = PostgresIndexName::new(index_name);
        let query = format!("INSERT INTO \"{index_name}\" (id, namespace, extractor, index_name, data, content_id, parent_content_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;");
        let _ = sqlx::query(&query)
            .bind(metadata.id)
            .bind(namespace)
            .bind(metadata.extractor_name)
            .bind(index_name.to_string())
            .bind(metadata.metadata)
            .bind(metadata.content_id)
            .bind(metadata.parent_content_id)
            .bind(timestamp_secs() as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_metadata(
        &self,
        namespace: &str,
        index_table_name: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>> {
        let index_table_name = PostgresIndexName::new(index_table_name);
        let query = format!(
            "SELECT * FROM \"{index_table_name}\" WHERE namespace = $1 and content_id = $2"
        );
        let rows = sqlx::query(&query)
            .bind(namespace)
            .bind(content_id)
            .fetch_all(&self.pool)
            .await?;

        let mut extracted_attributes = Vec::new();
        for row in rows {
            let id: String = row.get(0);
            let extractor: String = row.get(2);
            let data: serde_json::Value = row.get(4);
            let content_id: String = row.get(5);
            let parent_content_id: String = row.get(6);
            let attributes = ExtractedMetadata {
                id,
                extractor_name: extractor,
                metadata: data,
                content_id,
                parent_content_id,
            };
            extracted_attributes.push(attributes);
        }
        Ok(extracted_attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_metadata() {
        let index_manager =
            PostgresIndexManager::new("postgres://postgres:postgres@localhost:5432/indexify")
                .unwrap();
        let index_name = "test_index";
        let table_name = "test_table";
        let _ = index_manager
            .create_index(index_name, table_name)
            .await
            .unwrap();
        let metadata = ExtractedMetadata {
            id: "test_id".into(),
            content_id: "test_content_id".into(),
            parent_content_id: "test_parent_content_id".into(),
            metadata: serde_json::json!({"test": "test"}),
            extractor_name: "test_extractor".into(),
        };
        let namespace = "test_namespace";
        index_manager
            .add_metadata(namespace, table_name, metadata.clone())
            .await
            .unwrap();

        // Retreive the metadata from the database
        let metadata_out = index_manager
            .get_metadata(namespace, table_name, "test_content_id")
            .await
            .unwrap();

        assert_eq!(metadata_out.len(), 1);
        assert_eq!(metadata_out[0], metadata);
    }
}
