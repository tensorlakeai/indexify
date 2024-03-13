use std::{
    fmt,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use super::{table_name, ExtractedMetadata, MetadataStorage};
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
        let query = format!("INSERT INTO \"{table_name}\" (id, namespace, extractor, extractor_policy, index_name, data, content_id, parent_content_id, created_at) VALUES ($1, $2, $3, $9, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;");
        let _ = sqlx::query(&query)
            .bind(metadata.id)
            .bind(namespace)
            .bind(metadata.extractor_name)
            .bind(table_name.to_string())
            .bind(metadata.metadata)
            .bind(metadata.content_id)
            .bind(metadata.parent_content_id)
            .bind(timestamp_secs() as i64)
            .bind(metadata.extraction_policy)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_metadata(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>> {
        let index_table_name = PostgresIndexName::new(&table_name(namespace));
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
            let extraction_policy: String = row.get(3);
            let data: serde_json::Value = row.get(5);
            let content_id: String = row.get(6);
            let parent_content_id: String = row.get(7);
            let attributes = ExtractedMetadata {
                id,
                content_id,
                parent_content_id,
                metadata: data,
                extractor_name: extractor,
                extraction_policy,
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
        let namespace = "test_namespace";
        index_manager
            .create_metadata_table(namespace)
            .await
            .unwrap();
        let metadata = ExtractedMetadata {
            id: "test_id".into(),
            content_id: "test_content_id".into(),
            parent_content_id: "test_parent_content_id".into(),
            metadata: serde_json::json!({"test": "test"}),
            extractor_name: "test_extractor".into(),
            extraction_policy: "test_extractor_policy".into(),
        };
        index_manager
            .add_metadata(namespace, metadata.clone())
            .await
            .unwrap();

        // Retreive the metadata from the database
        let metadata_out = index_manager
            .get_metadata(namespace, "test_content_id")
            .await
            .unwrap();

        assert_eq!(metadata_out.len(), 1);
        assert_eq!(metadata_out[0], metadata);
    }
}
