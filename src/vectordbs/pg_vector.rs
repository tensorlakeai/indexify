use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use indexify_internal_api::ContentMetadata;
use pgvector::Vector;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};
use crate::{server_config::PgVectorConfig, utils::PostgresIndexName, vectordbs::FilterOperator};

#[derive(Debug)]
pub struct PgVector {
    config: PgVectorConfig,
    pool: Pool<Postgres>,
}

impl PgVector {
    pub async fn new(config: PgVectorConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&config.addr)?;
        Ok(Self { config, pool })
    }
}

/// Please note that only vectors with a dimension of up to dims=2000 can be
/// indexed! Can include much more customization if required later on
/// See https://github.com/pgvector/pgvector#approximate-search for more options
#[async_trait]
impl VectorDb for PgVector {
    /// we create a new table for each index.
    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<()> {
        if let Err(err) = sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
            .execute(&self.pool)
            .await
        {
            tracing::error!("Failed to create vector extension: {}", err);
            return Err(anyhow!("Failed to create vector extension {}", err));
        }
        let index_name = PostgresIndexName::new(&index.vectordb_index_name);
        let vector_dim = index.vector_dim;
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "vector_l2_ops",
            crate::vectordbs::IndexDistance::Cosine => "vector_cosine_ops",
            crate::vectordbs::IndexDistance::Dot => "vector_ip_ops",
        };

        let query = format!("CREATE TABLE IF NOT EXISTS \"{index_name}\"(content_id VARCHAR(1024) PRIMARY KEY, embedding vector({vector_dim}), metadata JSONB, root_content_metadata JSONB, content_metadata JSONB);", index_name = index_name, vector_dim = vector_dim);
        if let Err(err) = sqlx::query(&query).execute(&self.pool).await {
            tracing::error!("Failed to create table: {}, query: {}", err, query);
            return Err(anyhow!("Failed to create table {}", err));
        }
        let query = format!("CREATE INDEX IF NOT EXISTS \"{index_name}_hnsw\" ON \"{index_name}\" USING hnsw(embedding {distance_extension}) WITH (m = {}, ef_construction = {});",
            self.config.m, self.config.efconstruction
        );
        if let Err(err) = sqlx::query(&query).execute(&self.pool).await {
            tracing::error!("Failed to create index: {}, query: {}", err, query);
            return Err(anyhow!("Failed to create index {}", err));
        }
        Ok(())
    }

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        let index = PostgresIndexName::new(index);

        for chunk in chunks {
            let embedding = Vector::from(chunk.embedding);
            let query = format!("INSERT INTO \"{index}\"(content_id, embedding, metadata, root_content_metadata, content_metadata) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (content_id) DO UPDATE SET embedding = $2, metadata = $3, root_content_metadata = $4, content_metadata = $5;",);
            let root_content_metadata = serde_json::to_value(chunk.root_content_metadata)?;
            let chunk_metadata = serde_json::to_value(chunk.metadata)?;
            let content_metadata = serde_json::to_value(chunk.content_metadata)?;
            let _ = sqlx::query(&query)
                .bind(chunk.content_id)
                .bind(embedding)
                .bind(chunk_metadata)
                .bind(root_content_metadata)
                .bind(content_metadata)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn get_points(&self, index: &str, ids: Vec<String>) -> Result<Vec<VectorChunk>> {
        let index = PostgresIndexName::new(index);
        let mut chunks = Vec::new();

        for id in ids {
            let query = format!(
                "SELECT content_id, embedding, metadata, root_content_metadata, content_metadata FROM \"{index}\" WHERE content_id = $1;"
            );
            let row: Option<(
                String,
                Vector,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
                Option<serde_json::Value>,
            )> = sqlx::query_as(&query)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

            if let Some(row) = row {
                let metadata = row
                    .2
                    .map(|v| {
                        let cm: Result<HashMap<String, serde_json::Value>> =
                            serde_json::from_value(v)
                                .map_err(|e| anyhow!("Failed to deserialize metadata: {}", e));
                        if let Err(err) = &cm {
                            tracing::error!("{}", err.to_string());
                        }
                        cm.unwrap_or_default()
                    })
                    .unwrap_or_default();
                let root_content_metadata = row.3.map(|v| {
                    let cm: Result<ContentMetadata> = serde_json::from_value(v)
                        .map_err(|e| anyhow!("Failed to deserialize root_content_metadata: {}", e));
                    cm
                });
                if let Some(Err(err)) = root_content_metadata {
                    tracing::error!("{}", err.to_string());
                    continue;
                }
                let root_content_matadata = root_content_metadata.map(|v| v.unwrap());
                let content_metadata = row.4.map(|v| {
                    let cm: Result<ContentMetadata> = serde_json::from_value(v)
                        .map_err(|e| anyhow!("Failed to deserialize content_metadata: {}", e));
                    cm
                });
                if let Some(Err(err)) = &content_metadata {
                    tracing::error!("{}", err.to_string());
                    continue;
                }
                chunks.push(VectorChunk {
                    content_id: row.0,
                    embedding: row.1.into(),
                    metadata,
                    root_content_metadata: root_content_matadata,
                    content_metadata: content_metadata.unwrap().unwrap(),
                });
            }
        }

        Ok(chunks)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let index = PostgresIndexName::new(index);
        let query = format!("UPDATE {} SET metadata = $2 WHERE content_id = $1", index);
        let metadata = serde_json::to_value(metadata)?;
        let _rows_affected = sqlx::query(&query)
            .bind(content_id)
            .bind(metadata)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()> {
        let index = PostgresIndexName::new(index);
        let query = format!("DELETE FROM {} WHERE content_id = $1", index);
        let _rows_affected = sqlx::query(&query)
            .bind(content_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
        filters: Vec<super::Filter>,
    ) -> Result<Vec<SearchResult>> {
        let index = PostgresIndexName::new(&index);
        let mut query = format!(
            "SELECT content_id, CAST(1 - ($1 <=> embedding) AS FLOAT4) AS confidence_score, metadata, root_content_metadata, content_metadata FROM \"{index}\""
        );
        if !filters.is_empty() {
            query.push_str(" WHERE ");
            let filter_query = filters
                .iter()
                .map(|filter| {
                    format!(
                        "metadata->>'{}' {} '{}'",
                        filter.key,
                        match filter.operator {
                            FilterOperator::Eq => "=",
                            FilterOperator::Neq => "<>",
                        },
                        filter.value
                    )
                })
                .collect::<Vec<String>>()
                .join(" AND ");
            query.push_str(&filter_query);
        }
        query.push_str(&format!(" ORDER BY embedding <=> $1 LIMIT {k};"));
        // TODO: confidence_score is a distance here, let's make sure that similarity /
        // distance is the same across vectors databases
        let embedding = Vector::from(query_embedding);
        let rows = sqlx::query(&query)
            .bind(embedding)
            .fetch_all(&self.pool)
            .await?;
        let mut results: Vec<SearchResult> = Vec::new();
        for row in rows {
            let content_id: String = row.get(0);
            let confidence_score: f32 = row.get(1);
            let metadata: serde_json::Value = row.get(2);
            let root_content_metadata: serde_json::Value = row.get(3);
            let content_metadata: serde_json::Value = row.get(4);
            let metadata: HashMap<String, serde_json::Value> = serde_json::from_value(metadata)
                .map_err(|e| anyhow!("Failed to deserialize metadata: {}", e))?;
            let root_content_metadata: Option<ContentMetadata> =
                serde_json::from_value(root_content_metadata)
                    .map_err(|e| anyhow!("Failed to deserialize root_content_metadata: {}", e))?;
            let content_metadata: ContentMetadata = serde_json::from_value(content_metadata)
                .map_err(|e| anyhow!("Failed to deserialize content_metadata: {}", e))?;
            results.push(SearchResult {
                content_id,
                confidence_score,
                metadata,
                root_content_metadata,
                content_metadata,
            });
        }
        Ok(results)
    }

    // TODO: Should change index to &str to keep things uniform across functions
    #[tracing::instrument]
    async fn drop_index(&self, index: &str) -> Result<()> {
        let index = PostgresIndexName::new(index);
        let query = format!("DROP TABLE IF EXISTS \"{index}\";");
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let index = PostgresIndexName::new(index);
        let query = format!("SELECT COUNT(*) FROM \"{index}\";");
        let result = sqlx::query(&query).fetch_one(&self.pool).await?;
        let count: i64 = result.get(0);
        Ok(count as u64)
    }

    fn name(&self) -> String {
        "pg_vector".into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::CreateIndexParams;
    use crate::{
        server_config::PgVectorConfig,
        vectordbs::{
            pg_vector::PgVector,
            tests::{basic_search, crud_operations, insertion_idempotent, search_filters},
            IndexDistance,
            VectorDBTS,
        },
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        // Create the sea-orm connection, s.t. we can share it with the
        // application-level pool
        let index_name = "index_default.minil6.embedding";
        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let vector_db: VectorDBTS = Arc::new(
            PgVector::new(PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            })
            .await
            .unwrap(),
        );
        // Drop index (this is idempotent)
        vector_db.drop_index(index_name).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.to_string(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        basic_search(vector_db, index_name).await;
    }

    #[tokio::test]
    async fn test_store_metadata() {
        let index_name = "index_default.minil6.embedding";
        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let vector_db: VectorDBTS = Arc::new(
            PgVector::new(PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            })
            .await
            .unwrap(),
        );
        // Drop index (this is idempotent)
        vector_db.drop_index(index_name).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.to_string(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "index_default.minil6.embedding";
        let hash_on = vec!["user_id".to_string(), "url".to_string()];

        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let vector_db: VectorDBTS = Arc::new(
            PgVector::new(PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            })
            .await
            .unwrap(),
        );

        vector_db.drop_index(index_name).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        insertion_idempotent(vector_db, index_name).await;
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn setup_vector_db() {
        let index_name = "index_default.minil6.embedding";
        let hash_on = vec!["user_id".to_string(), "url".to_string()];

        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let vector_db: VectorDBTS = Arc::new(
            PgVector::new(PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            })
            .await
            .unwrap(),
        );

        vector_db.drop_index(index_name).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        crud_operations(vector_db, index_name).await;
    }

    #[tokio::test]
    async fn test_search_filters() {
        let index_name = "index_default.minil6.embedding";
        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let vector_db: VectorDBTS = Arc::new(
            PgVector::new(PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            })
            .await
            .unwrap(),
        );
        // Drop index (this is idempotent)
        vector_db.drop_index(index_name).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.to_string(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        search_filters(vector_db, index_name).await;
    }
}
