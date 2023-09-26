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
    pub fn new(config: PgEmbeddingConfig, db_conn: DbConn) -> PgEmbeddingDb {
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

        // TODO: @diptanu, the "id" here corresponds to the chunk-id, and is NOT SERIAL, right?
        let mut query = r#"
            CREATE TABLE $1(id integer PRIMARY KEY , embedding real[]);
            CREATE INDEX ON $1 USING hnsw(embedding {distance_extension}) WITH (dims={dims}, m={m}, efconstruction={efconstruction}, efsearch={efsearch});
            SET enable_seqscan = off;
        )"#;
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [],
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        if exec_res.rows_affected() == 0 {
            Err(VectorDbError::IndexCreationError(format!(
                "No tables affected when deleting: {:?}",
                index_name.clone()
            )))
        } else if exec_res.rows_affected() == 1 {
            Ok(())
        } else {
            Err(VectorDbError::IndexCreationError(format!(
                "More than one table affected! {:?} {:?}",
                index_name.clone(),
                exec_res.rows_affected()
            )))
        }
    }

    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError> {
        // Due to the limitations of sea-query (we cannot encode tuples, nor can we encode arrays of arrays)
        // We iteratively build out the query manually
        // Insert an array into the index
        let flattened_chunks = chunks
            .into_iter()
            .map(|x| {
                (x.chunk_id, x.embeddings)
                // It is very disgusting, but let's just run each insert one by one.
                // TODO: Revisit this, @diptanu maybe you have ideas
            })
            .collect::<Vec<(String, Vec<f32>)>>();
        // String serialization seems to be the most fitting solution for sea-orm, let's go with this for now, if it's bad we'll revisit ... May need to use sqlx or similar for speed
        let values_str: String = flattened_chunks
            .iter()
            .map(|(id, embedding)| {
                let embedding_str = embedding
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("('{}', ARRAY[{}])", id, embedding_str)
            })
            .join(", ");
        let query = r#"
            INSERT INTO $1 (id, embedding) VALUES {values_str};
        )"#;
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [
                    // WIP, but doesn't seem to be supported sea_orm::Value::Array(ArrayType::), // sea_query::ValueTuple(sea_query::Value::String, sea_query::Value::Array((), ())), Some(Box::new())
                    sea_orm::Value::String(Some(Box::new(index.to_string()))),
                ],
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexDeletionError(index.to_string(), format!("{:?}", e))
            })?;
        if exec_res.rows_affected() == 0 {
            Err(VectorDbError::IndexDeletionError(
                index.to_string(),
                "No tables affected when deleting".to_string(),
            ))
        } else if exec_res.rows_affected() == 1 {
            Ok(())
        } else {
            Err(VectorDbError::IndexDeletionError(
                index.to_string(),
                format!(
                    "More than one table affected! {:?}",
                    exec_res.rows_affected()
                ),
            ))
        }
    }

    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>, VectorDbError> {
        // TODO: How to turn query_embedding into an array[...]
        let query = r#"
            SELECT id FROM $1 ORDER BY embedding <-> $2 LIMIT $3;
        )"#;
        let query_embedding = query_embedding
            .into_iter()
            .map(|x| sea_orm::Value::Float(Some(x)))
            .collect();
        SearchResult::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            query,
            [
                sea_orm::Value::String(Some(Box::new(index.to_string()))),
                sea_orm::Value::Array(sea_query::ArrayType::Float, Some(Box::new(query_embedding))),
                sea_orm::Value::BigUnsigned(Some(k)),
            ],
        ))
        .all(&self.db_conn)
        .await
        .map_err(|e| VectorDbError::IndexReadError(format!("{:?}: {:?}", index, e)))
    }

    // TODO: Should change index to &str
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let query = r#"
            DROP TABLE $1;
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
            SELECT COUNT(*) FROM TABLE $1;
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
