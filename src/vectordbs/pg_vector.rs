use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use pgvector::Vector;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, Row};

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};
use crate::server_config::PgVectorConfig;

#[derive(Debug, Clone)]
pub struct IndexName(String);

impl IndexName {
    pub fn new(index_name: &str) -> IndexName {
        let name = index_name.replace('-', "_");
        Self(name)
    }
}

impl fmt::Display for IndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct PgVector {
    config: PgVectorConfig,
    pool: Pool<Postgres>,
}

impl PgVector {
    pub async fn new(config: PgVectorConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.addr)
            .await?;
        Ok(Self { config, pool })
    }
}

/// Acts as a salt, to make sure there are no collisions with other tables
const INDEX_TABLE_PREFIX: &str = "index_";

/// Please note that only vectors with a dimension of up to dims=2000 can be
/// indexed! Can include much more customization if required later on
/// See https://github.com/pgvector/pgvector#approximate-search for more options
#[async_trait]
impl VectorDb for PgVector {
    fn name(&self) -> String {
        "pg_vector".into()
    }

    /// we create a new table for each index.
    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<()> {
        let index_name = IndexName::new(&index.vectordb_index_name);
        let vector_dim = index.vector_dim;
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "vector_l2_ops",
            crate::vectordbs::IndexDistance::Cosine => "vector_cosine_ops",
            crate::vectordbs::IndexDistance::Dot => "vector_ip_ops",
        };

        let query = format!("CREATE TABLE IF NOT EXISTS {INDEX_TABLE_PREFIX}{index_name}(content_id VARCHAR(1024) PRIMARY KEY , embedding vector({vector_dim}));",);

        let _ = sqlx::query(&query).execute(&self.pool).await?;
        let query = format!("CREATE INDEX IF NOT EXISTS {INDEX_TABLE_PREFIX}{index_name}_hnsw ON {INDEX_TABLE_PREFIX}{index_name} USING hnsw(embedding {distance_extension}) WITH (m = {}, ef_construction = {});",
            self.config.m, self.config.efconstruction
        );
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        let index = IndexName::new(index);

        for chunk in chunks {
            let embedding = Vector::from(chunk.embedding);
            let query = format!("INSERT INTO {INDEX_TABLE_PREFIX}{index}(content_id, embedding) VALUES ($1, $2) ON CONFLICT (content_id) DO UPDATE SET embedding = $2;",);
            let _ = sqlx::query(&query)
                .bind(chunk.content_id)
                .bind(embedding)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>> {
        let index = IndexName::new(&index);
        let query = format!(
            "SELECT content_id, CAST(1 - ($1 <-> embedding) AS FLOAT4) AS confidence_score FROM {INDEX_TABLE_PREFIX}{index} ORDER BY embedding <-> $1 LIMIT {k};"
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
    async fn drop_index(&self, index: String) -> Result<()> {
        let index = IndexName::new(&index);
        let query = format!("DROP TABLE IF EXISTS {INDEX_TABLE_PREFIX}{index};");
        let _ = sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let index = IndexName::new(index);
        let query = format!("SELECT COUNT(*) FROM {INDEX_TABLE_PREFIX}{index};");
        let result = sqlx::query(&query).fetch_one(&self.pool).await?;
        let count: i64 = result.get(0);
        Ok(count as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::CreateIndexParams;
    use crate::{
        server_config::PgVectorConfig,
        vectordbs::{pg_vector::PgVector, IndexDistance, VectorChunk, VectorDBTS},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        // Create the sea-orm connection, s.t. we can share it with the
        // application-level pool
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
        vector_db.drop_index("hello-index".into()).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
        };
        vector_db
            .add_embedding("hello-index", vec![chunk])
            .await
            .unwrap();

        let results = vector_db
            .search("hello-index".into(), vec![10., 8.], 1)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "idempotent-index";
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

        vector_db.drop_index(index_name.into()).await.unwrap();
        vector_db
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
                vector_dim: 2,
                distance: IndexDistance::Cosine,
                unique_params: Some(hash_on.clone()),
            })
            .await
            .unwrap();
        let chunk = VectorChunk {
            content_id: "0".into(),
            embedding: vec![0., 2.],
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
}
