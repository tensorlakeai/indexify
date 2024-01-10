use std::{fmt, sync::Arc};

use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use crate::{
    coordinator_client::CoordinatorClient,
    grpc_helper::GrpcHelper,
    indexify_coordinator::{CreateIndexRequest, Index},
    persistence::ExtractedAttributes,
    utils::timestamp_secs,
};

pub struct AttributeIndexManager {
    pool: Pool<Postgres>,
    coordinator_client: Arc<CoordinatorClient>,
}

impl fmt::Debug for AttributeIndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttributeIndexManager").finish()
    }
}

impl AttributeIndexManager {
    pub async fn new(db_addr: &str, coordinator_client: Arc<CoordinatorClient>) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(db_addr)
            .await?;
        Ok(Self {
            pool,
            coordinator_client,
        })
    }

    pub async fn create_index(
        &self,
        repository: &str,
        index_name: &str,
        extractor: &str,
        extractor_binding: &str,
        schema: serde_json::Value,
    ) -> Result<String> {
        let table_name = format!("structured_store_{repository}_{index_name}");
        let index = CreateIndexRequest {
            index: Some(Index {
                name: index_name.to_string(),
                table_name,
                repository: repository.to_string(),
                schema: schema.to_string(),
                extractor: extractor.to_string(),
                extractor_binding: extractor_binding.to_string(),
            }),
        };
        let query = "CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT PRIMARY KEY,
            repository_id TEXT,
            extractor TEXT,
            index_name TEXT,
            data JSONB,
            content_id TEXT,
            created_at BIGINT,
        );";
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        let req = GrpcHelper::into_req(index);
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_index(req)
            .await?;
        Ok(index_name.to_string())
    }

    pub async fn add_index(
        &self,
        repository: &str,
        index_name: &str,
        extracted_attributes: ExtractedAttributes,
    ) -> Result<()> {
        let table_name = format!("structured_store_{repository}_{index_name}");
        let query = format!("INSERT INTO {table_name} (id, repository_id, extractor, index_name, data, content_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7);");
        let _ = sqlx::query(&query)
            .bind(extracted_attributes.id)
            .bind(repository)
            .bind(extracted_attributes.extractor_name)
            .bind(index_name)
            .bind(extracted_attributes.attributes)
            .bind(extracted_attributes.content_id)
            .bind(timestamp_secs() as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_attributes(
        &self,
        repository: &str,
        index_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedAttributes>> {
        let table_name = format!("structured_store_{repository}_{index_name}");
        let query = format!("SELECT * FROM {table_name} WHERE repository_id = $1 AND index_name = $2 AND content_id = $3;");
        let rows = sqlx::query(&query)
            .bind(repository)
            .bind(index_name)
            .bind(content_id)
            .fetch_all(&self.pool)
            .await?;
        let mut extracted_attributes = Vec::new();
        for row in rows {
            let id: String = row.get(0);
            let extractor: String = row.get(2);
            let data: serde_json::Value = row.get(4);
            let content_id: String = row.get(5);
            let attributes = ExtractedAttributes {
                id,
                extractor_name: extractor,
                attributes: data,
                content_id,
            };
            extracted_attributes.push(attributes);
        }
        Ok(extracted_attributes)
    }
}
