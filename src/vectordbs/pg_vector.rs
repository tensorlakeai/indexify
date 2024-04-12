use anyhow::{anyhow, Result};
use async_trait::async_trait;
use pgvector::Vector;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};
use crate::{server_config::PgVectorConfig, utils::PostgresIndexName};

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

        let query = format!("CREATE TABLE IF NOT EXISTS \"{index_name}\"(content_id VARCHAR(1024) PRIMARY KEY, embedding vector({vector_dim}), metadata JSONB);", index_name = index_name, vector_dim = vector_dim);
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
            let query = format!("INSERT INTO \"{index}\"(content_id, embedding, metadata) VALUES ($1, $2, $3) ON CONFLICT (content_id) DO UPDATE SET embedding = $2, metadata = $3;",);
            let _ = sqlx::query(&query)
                .bind(chunk.content_id)
                .bind(embedding)
                .bind(chunk.metadata)
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
                "SELECT content_id, embedding, metadata FROM \"{index}\" WHERE content_id = $1;"
            );
            let row: Option<(String, Vector, Option<serde_json::Value>)> = sqlx::query_as(&query)
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;

            if let Some(row) = row {
                chunks.push(VectorChunk {
                    content_id: row.0,
                    embedding: row.1.into(),
                    metadata: row.2.unwrap_or_default(),
                });
            }
        }

        Ok(chunks)
    }

    async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: serde_json::Value,
    ) -> Result<()> {
        let index = PostgresIndexName::new(index);
        let query = format!("UPDATE {} SET metadata = $2 WHERE content_id = $1", index);
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
    ) -> Result<Vec<SearchResult>> {
        let index = PostgresIndexName::new(&index);
        let query = format!(
            "SELECT content_id, CAST(1 - ($1 <-> embedding) AS FLOAT4) AS confidence_score FROM \"{index}\" ORDER BY embedding <-> $1 LIMIT {k};"
        );
        // TODO: confidence_score is a distance here, let's make sure that similarity /
        // distance is the same across vectors databases
        let embedding = Vector::from(query_embedding);
        let rows = sqlx::query(&query)
            .bind(embedding)
            .fetch_all(&self.pool)
            .await?;
        let results = rows
            .into_iter()
            .map(|row| {
                let content_id: String = row.get(0);
                let confidence_score: f32 = row.get(1);
                SearchResult {
                    content_id,
                    confidence_score,
                }
            })
            .collect();
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

    use serde_json::json;

    use super::CreateIndexParams;
    use crate::{
        data_manager::DataManager,
        server_config::PgVectorConfig,
        vectordbs::{pg_vector::PgVector, IndexDistance, VectorChunk, VectorDBTS},
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
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
        };
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();

        let results = vector_db
            .search(index_name.into(), vec![10., 8.], 1)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    fn make_id() -> String {
        DataManager::make_id("namespace", &nanoid::nanoid!(), &None)
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

        let content_ids = vec![make_id(), make_id()];
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk1 = VectorChunk {
            content_id: content_ids[0].clone(),
            embedding: vec![0.1, 0.2],
            metadata: metadata1.clone(),
        };
        vector_db
            .add_embedding(index_name, vec![chunk1])
            .await
            .unwrap();
        let metadata2 = json!({"key1": "value3", "key2": "value4"});
        let chunk2 = VectorChunk {
            content_id: content_ids[1].clone(),
            embedding: vec![0.3, 0.4],
            metadata: metadata2.clone(),
        };
        vector_db
            .add_embedding(index_name, vec![chunk2])
            .await
            .unwrap();

        let result = vector_db
            .get_points(index_name, content_ids.clone())
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for chunk in result {
            if chunk.content_id == content_ids[0] {
                assert_eq!(chunk.metadata, metadata1);
            } else if chunk.content_id == content_ids[1] {
                assert_eq!(chunk.metadata, metadata2);
            } else {
                panic!("unexpected content_id: {}", chunk.content_id);
            }
        }

        let new_metadata = json!({"key1": "value5", "key2": "value6"});
        vector_db
            .update_metadata(index_name, content_ids[0].clone(), new_metadata.clone())
            .await
            .unwrap();
        let result = vector_db
            .get_points(index_name, vec![content_ids[0].clone()])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].metadata, new_metadata);
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
        let metadata1 = json!({"key1": "value1", "key2": "value2"});
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
            metadata: metadata1.clone(),
        };
        vector_db
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
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
        let content_id = "0";
        let chunk = VectorChunk {
            content_id: content_id.into(),
            embedding: vec![0., 2.],
            metadata: json!({"key1": "value1", "key2": "value2"}),
        };
        vector_db
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        vector_db
            .add_embedding(index_name, vec![chunk])
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 1);

        vector_db
            .remove_embedding(index_name, content_id)
            .await
            .unwrap();
        let num_elements = vector_db.num_vectors(index_name).await.unwrap();
        assert_eq!(num_elements, 0);
    }
}
