use async_trait::async_trait;
use sea_orm::{query::JsonValue, ConnectionTrait, ExecResult, FromQueryResult};
use serde_json::Value;

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb, VectorDbError};
use crate::PgEmbeddingConfig;
use itertools::Itertools;
use sea_orm::{self, DbBackend, DbConn, Statement};

/// PgEmbeddingDb
pub struct PgEmbeddingDb {
    pg_embedding_config: PgEmbeddingConfig,
    db_conn: DbConn,
}

pub struct PgEmbeddingPayload {
    pub text: String,
    pub chunk_id: String,
    pub metadata: serde_json::Value,
}

impl PgEmbeddingDb {
    pub async fn new(config: PgEmbeddingConfig, db_conn: DbConn) -> PgEmbeddingDb {
        // Create a new pool based on the config

        Self {
            pg_embedding_config: config,
            db_conn,
        }
    }

    // Create a client, I suppose this will just be a postgres client (?)
    // pub fn create_client(&self) -> Result<>
}

#[async_trait]
impl VectorDb for PgEmbeddingDb {
    fn name(&self) -> String {
        "pg_embedding".into()
    }

    /// Because this is a separate VectorDb index (i.e. modularly replacdable by qdrant),
    /// we create a new table for each index.
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError> {
        // TODO: Assert or choose optional parameters
        let index_name = index.vectordb_index_name;
        let vector_dim = index.vector_dim;
        let (m, efconstruction, efsearch) = match index.unique_params {
            Some(unique_params) => {
                // Return error if the default parameters are not.
                // We can also return some default's instead, but I suppose the "Option<>" wrapper implies that either all are set, or none are
                unique_params
                    .iter()
                    .map(|x| x.parse::<i32>().unwrap())
                    .collect_tuple()
                    .unwrap()
            }
            None => (3, 5, 5),
        };

        // Depending on the distance, we have to create a different index ...
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "",
            crate::vectordbs::IndexDistance::Cosine => "ann_cos_ops",
            crate::vectordbs::IndexDistance::Dot => "ann_manhattan_ops",
        };

        let mut query = r#"
            CREATE TABLE {index_name}(id integer PRIMARY KEY , embedding real[]);
            CREATE INDEX ON {index_name} USING hnsw(embedding {distance_extension}) WITH (dims={dims}, m={m}, efconstruction={efconstruction}, efsearch={efsearch});
            SET enable_seqscan = off;
        )"#;
        // sqlx::query(query).execute(self.pool).await?;
        Ok(())
    }

    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError> {
        // Insert an array into the index
        let mut query = r#"
            INSERT INTO {index_name} VALUES
        )"#;
        // unroll vectors... maybe don't use strings, strings will make things slow!
        // TODO: What is a vector chunk
        // sqlx::query_as(query)
        //     .bind(chunks)
        //     .execute(self.pool)
        //     .await?;
        Ok(())
    }

    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>, VectorDbError> {
        // TODO: How to turn query_embedding into an array[...]
        let mut query = r#"
            SELECT id FROM {index} ORDER BY embedding <-> array[3,3,3] LIMIT {k};
        )"#;
        // sqlx::query_as(query)
        //     .bind(index, query_embedding, k)
        //     .fetch_all(&self.db_connol)
        //     .await
        //     .map_err(|e| e)
        todo!()
    }

    // TODO: Should change index to &str
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let query = r#"
            DROP TABLE {index_name};
        )"#;
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [sea_orm::Value::String(Some(Box::new(index.clone())))],
            ))
            .await
            .map_err(|e| VectorDbError::IndexDeletionError(index.clone(), format!("{:?}", e)))?;
        if exec_res.rows_affected() == 0 {
            Err(VectorDbError::IndexDeletionError(
                index.clone(),
                "No tables affected when deleting".to_string(),
            ))
        } else if exec_res.rows_affected() == 1 {
            Ok(())
        } else {
            Err(VectorDbError::IndexDeletionError(
                index.clone(),
                format!(
                    "More than one table affected! {:?}",
                    exec_res.rows_affected()
                ),
            ))
        }
    }

    async fn num_vectors(&self, index: &str) -> Result<u64, VectorDbError> {
        let query = r#"
            SELECT COUNT(*) FROM TABLE {index_name};
        )"#;
        let response: JsonValue = JsonValue::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            query,
            [sea_orm::Value::String(Some(Box::new(index.to_string())))],
        ))
        .one(&self.db_conn)
        .await
        .map_err(|e| VectorDbError::IndexReadError(format!("{:?}: {:?}", index, e)))?
        .ok_or(VectorDbError::IndexReadError(
            "num_vectors did not run successfully".to_string(),
        ))?;
        match response {
            Value::Number(n) => n.as_u64().ok_or(VectorDbError::IndexReadError(
                "COUNT(*) did not return a positive integer".to_string(),
            )),
            _ => Err(VectorDbError::IndexReadError(
                "COUNT(*) did not return Number".to_string(),
            )),
        }
    }
}
