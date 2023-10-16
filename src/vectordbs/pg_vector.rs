use async_trait::async_trait;
use sea_orm::{query::JsonValue, ConnectionTrait, ExecResult, FromQueryResult};
use serde_json::Value;
use tracing::{debug, warn};

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb, VectorDbError};
use crate::PgVectorConfig;
use itertools::Itertools;
use sea_orm::{self, DbBackend, DbConn, Statement};
use std::fmt;

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
    db_conn: DbConn,
}

impl PgVector {
    pub fn new(config: PgVectorConfig, db_conn: DbConn) -> PgVector {
        Self { config, db_conn }
    }
}

/// Acts as a salt, to make sure there are no collisions with other tables
const INDEX_TABLE_PREFIX: &str = "index_";

/// Please note that only vectors with a dimension of up to dims=2000 can be indexed!
/// Can include much more customization if required later on
/// See https://github.com/pgvector/pgvector#approximate-search for more options
#[async_trait]
impl VectorDb for PgVector {
    fn name(&self) -> String {
        "pg_vector".into()
    }

    /// we create a new table for each index.
    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError> {
        let index_name = IndexName::new(&index.vectordb_index_name);
        let vector_dim = index.vector_dim;
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "vector_l2_ops",
            crate::vectordbs::IndexDistance::Cosine => "vector_cosine_ops",
            crate::vectordbs::IndexDistance::Dot => "vector_ip_ops",
        };

        let query = r#"CREATE EXTENSION IF NOT EXISTS vector;"#;
        let _ = self
            .db_conn
            .execute(Statement::from_string(
                DbBackend::Postgres,
                query.to_string(),
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        // the "id" here corresponds to the chunk-id, and is NOT SERIAL
        let query = format!(
            r#"CREATE TABLE IF NOT EXISTS {INDEX_TABLE_PREFIX}{index_name}(chunk_id VARCHAR(1024) PRIMARY KEY , embedding vector({vector_dim}));"#,
        );
        self.db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;

        // Create index if dimensions is below 2000. It will be much slower for larger vectors, we should be explicit about this in the docs
        // This is a limitation of pg_vector
        if vector_dim >= 2000 {
            warn!(
                "You are trying to index vectors with dimension {}. This will be very slow.",
                vector_dim
            );
        }
        debug!("Parameters for index {} are: {:?}", index_name, self.config);
        // IF NOT EXISTS requires an index-name in postgres. "_{INDEX_TABLE_PREFIX}{index_name}_hnsw" is automatically set as the index-name
        let query = format!(
            r#"
                    CREATE INDEX IF NOT EXISTS _{INDEX_TABLE_PREFIX}{index_name}_hnsw ON {INDEX_TABLE_PREFIX}{index_name} USING hnsw(embedding {distance_extension}) WITH (m = {}, ef_construction = {});
                "#,
            self.config.m, self.config.efconstruction
        );
        let _ = self
            .db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;

        let query = format!(r#"SET hnsw.ef_search = {};"#, self.config.efsearch);
        let _ = self
            .db_conn
            .execute(Statement::from_string(
                DbBackend::Postgres,
                query.to_string(),
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;

        Ok(())
    }

    #[tracing::instrument]
    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError> {
        let index = IndexName::new(index);
        // Because sea-orm is not super flexible in accepting arrays of tuples, we build the query somewhat manually.
        // Indexing starts at 1 (with $1) in Postgres
        // We "unroll" tuples of (chunk_id, embedding).
        // After the table-name, every second item is the chunk_id.
        // After the table-name, and with an offset of 1 (because chunk_id is inserted), every second item is the embedding
        // The final query looks similar to:
        // INSERT INTO index_table (chunk_id, embedding) VALUES (chunk_id_1, embedding_1), (chunk_id_2, embedding_2), ..., (chunk_id_n, embedding_n);
        //                                                |>(1+2*0)=$1 |>(2+2*0)=$2  |>(1+2*1)=$3 |>(2+2*4)=$4       |>(1+2*n)    |>(2+2*n)
        let value_placeholders = chunks
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("(${}, ${})", 1 + 2 * idx, 2 + 2 * idx))
            .join(", ");
        let query = format!(
            r#"
                INSERT INTO {INDEX_TABLE_PREFIX}{index} (chunk_id, embedding) VALUES {value_placeholders}
                ON CONFLICT (chunk_id)
                DO UPDATE SET embedding = EXCLUDED.embedding;
            "#
        );
        // Due to the limitations of sea-query (we cannot encode tuples, nor can we encode arrays of arrays)
        // We iteratively build out the query manually
        let parameters = chunks
            .into_iter()
            .flat_map(|chunk| {
                vec![
                    sea_orm::Value::String(Some(Box::new(chunk.chunk_id))),
                    sea_orm::sea_query::Value::Array(
                        sea_orm::sea_query::ArrayType::Float,
                        Some(Box::new(
                            chunk
                                .embeddings
                                .into_iter()
                                .map(|x| sea_orm::Value::Float(Some(x)))
                                .collect(),
                        )),
                    ),
                ]
            })
            .collect::<Vec<sea_orm::Value>>();
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query.as_str(),
                parameters,
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexWriteError(format!("{:?} {:?}", index.to_string(), e))
            })?;
        if exec_res.rows_affected() == 0 {
            return Err(VectorDbError::IndexWriteError(format!(
                "{:?} {:?}",
                index.to_string(),
                "No rows were inserted".to_string(),
            )));
        }
        Ok(())
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>, VectorDbError> {
        let index = IndexName::new(&index);
        // TODO: confidence_score is a distance here, let's make sure that similarity / distance is the same across vectors databases
        let query = format!(
            r#"
            SELECT chunk_id, CAST(embedding <-> ($1)::vector AS FLOAT4) AS confidence_score FROM {INDEX_TABLE_PREFIX}{index} ORDER BY embedding <-> ($1)::vector LIMIT {k};
        "#
        );
        let query_embedding = query_embedding
            .into_iter()
            .map(|x| sea_orm::Value::Float(Some(x)))
            .collect();
        SearchResult::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            query.as_str(),
            [sea_orm::sea_query::Value::Array(
                sea_orm::sea_query::ArrayType::Float,
                Some(Box::new(query_embedding)),
            )],
        ))
        .all(&self.db_conn)
        .await
        .map_err(|e| VectorDbError::IndexReadError(format!("Search Error {:?}: {:?}", index, e)))
    }

    // TODO: Should change index to &str to keep things uniform across functions
    #[tracing::instrument]
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let index = IndexName::new(&index);
        let query = format!("DROP TABLE IF EXISTS {INDEX_TABLE_PREFIX}{index};");
        self.db_conn
            .execute(Statement::from_string(
                DbBackend::Postgres,
                query.to_string(),
            ))
            .await
            .map_err(|e| VectorDbError::IndexDeletionError(index.0, format!("{:?}", e)))?;
        Ok(())
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64, VectorDbError> {
        let index = IndexName::new(index);
        let query = format!("SELECT COUNT(*) FROM {INDEX_TABLE_PREFIX}{index};");
        let response: JsonValue =
            JsonValue::find_by_statement(Statement::from_string(DbBackend::Postgres, query))
                .one(&self.db_conn)
                .await
                .map_err(|e| {
                    VectorDbError::IndexReadError(format!("Num Vectors{:?}: {:?}", index, e))
                })?
                .ok_or(VectorDbError::IndexReadError(
                    "num_vectors did not run successfully".to_string(),
                ))?;
        match response {
            Value::Object(mut x) => {
                let x = x.remove("count").ok_or(VectorDbError::IndexReadError(
                    "SELECT COUNT(*) did not return a dictionary with key 'count'".to_string(),
                ))?;
                match x {
                    Value::Number(n) => n.as_u64().ok_or(VectorDbError::IndexReadError(
                        "COUNT(*) did not return a positive integer".to_string(),
                    )),
                    _ => Err(VectorDbError::IndexReadError(
                        "COUNT(*) did not return Number".to_string(),
                    )),
                }
            }
            _ => Err(VectorDbError::IndexReadError(
                "SELECT COUNT(*) did not an object".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::Database;

    use crate::vectordbs::{pg_vector::PgVector, IndexDistance, VectorChunk, VectorDBTS};

    use super::CreateIndexParams;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        // Create the sea-orm connection, s.t. we can share it with the application-level pool
        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let db_conn = Database::connect(database_url).await.unwrap();
        let vector_db: VectorDBTS = Arc::new(PgVector::new(
            crate::PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            },
            db_conn,
        ));
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
            chunk_id: "0".into(),
            embeddings: vec![0., 2.],
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
        let db_conn = Database::connect(database_url).await.unwrap();
        let vector_db: VectorDBTS = Arc::new(PgVector::new(
            crate::PgVectorConfig {
                addr: database_url.to_string(),
                m: 16,
                efconstruction: 64,
                efsearch: 40,
            },
            db_conn,
        ));

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
            chunk_id: "0".into(),
            embeddings: vec![0., 2.],
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
