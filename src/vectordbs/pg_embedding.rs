use async_trait::async_trait;
use sea_orm::{query::JsonValue, ConnectionTrait, ExecResult, FromQueryResult};
use serde_json::Value;

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb, VectorDbError};
use crate::PgEmbeddingConfig;
use itertools::Itertools;
use sea_orm::{self, DbBackend, DbConn, Statement};

/// PgEmbeddingDb
pub struct PgEmbedding {
    config: PgEmbeddingConfig,
    db_conn: DbConn,
}

pub struct PgEmbeddingPayload {
    pub chunk_id: String,
    pub metadata: serde_json::Value,
}

impl PgEmbedding {
    pub fn new(config: PgEmbeddingConfig, db_conn: DbConn) -> PgEmbedding {
        Self { config, db_conn }
    }
}

const INDEX_TABLE_PREFIX: &'static str = "index_";

#[async_trait]
impl VectorDb for PgEmbedding {
    fn name(&self) -> String {
        "pg_embedding".into()
    }

    /// Because this is a separate VectorDb index (i.e. modularly replacable by qdrant),
    /// we create a new table for each index.
    async fn create_index(&self, index: CreateIndexParams) -> Result<(), VectorDbError> {
        let index_name = index.vectordb_index_name;
        let vector_dim = index.vector_dim;
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "",
            crate::vectordbs::IndexDistance::Cosine => "ann_cos_ops",
            crate::vectordbs::IndexDistance::Dot => "ann_manhattan_ops",
        };

        // the "id" here corresponds to the chunk-id, and is NOT SERIAL
        let query = r#"
            CREATE TABLE $1(id integer PRIMARY KEY , embedding real[]);
            CREATE INDEX ON $1 USING hnsw(embedding {distance_extension}) WITH (dims={vector_dim}, m={self.pg_embedding_config.m}, efconstruction={self.pg_embedding_config.efconstruction}, efsearch={self.pg_embedding_config.efsearch});
            SET enable_seqscan = off;
        )"#;
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                [sea_orm::Value::String(Some(Box::new(format!(
                    "{:?}{:?}",
                    INDEX_TABLE_PREFIX, index_name
                ))))],
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        if exec_res.rows_affected() <= 1 {
            // Create's are idempotent. If no tables are affected (i.e. added), Ok(()) is returned
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
        // Because sea-orm is not super flexible in accepting arrays of tuples, we build the query somewhat manually.
        // Indexing starts at 1 (with $1) in Postgres
        // The first parameter is the table-name (as you can see in the below "query" variable)
        // Then, we "unroll" tuples of (chunk_id, embedding).
        // After the table-name, every second item is the chunk_id.
        // After the table-name, and with an offset of 1 (because chunk_id is inserted), every second item is the embedding
        // The final query looks similar to:
        // INSERT INTO index_table (id, embedding) VALUES (chunk_id_1, embedding_1), (chunk_id_2, embedding_2), ..., (chunk_id_n, embedding_n);
        //             |------> $1                        |>(2+2*0)=$2 |>(3+2*0)=$3  |>(2+2*1)=$4 |>(3+2*4)=$5       |>(2+2*n)    |>(3+2*n)
        let _value_placeholders = chunks
            .iter()
            .enumerate()
            .map(|(idx, _)| format!("(${}, ${})", 2 + 2 * idx, 3 + 2 * idx))
            .join(", ");
        let query = r#"
            INSERT INTO $1 (id, embedding) VALUES {_value_placeholders} 
            ON CONFLICT (id) 
            DO UPDATE SET embedding = EXCLUDED.embedding;
        )"#;
        // Due to the limitations of sea-query (we cannot encode tuples, nor can we encode arrays of arrays)
        // We iteratively build out the query manually
        let mut parameters = chunks
            .into_iter()
            .flat_map(|chunk| {
                vec![
                    sea_orm::Value::String(Some(Box::new(chunk.chunk_id))),
                    sea_orm::Value::Array(
                        sea_query::ArrayType::Float,
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
        parameters.insert(
            0,
            sea_orm::Value::String(Some(Box::new(format!(
                "{:?}{:?}",
                INDEX_TABLE_PREFIX, index
            )))),
        );
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                parameters,
            ))
            .await
            .map_err(|e| {
                VectorDbError::IndexWriteError(format!("{:?} {:?}", index.to_string(), e))
            })?;
        if exec_res.rows_affected() == 0 {
            Err(VectorDbError::IndexWriteError(format!(
                "{:?} {:?}",
                index.to_string(),
                "No rows were inserted".to_string(),
            )))
        } else {
            Ok(())
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
                sea_orm::Value::String(Some(Box::new(format!(
                    "{:?}{:?}",
                    INDEX_TABLE_PREFIX, index
                )))),
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
                [sea_orm::Value::String(Some(Box::new(format!(
                    "{:?}{:?}",
                    INDEX_TABLE_PREFIX, index
                ))))],
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
            [sea_orm::Value::String(Some(Box::new(format!(
                "{:?}{:?}",
                INDEX_TABLE_PREFIX, index
            ))))],
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::Database;

    use crate::vectordbs::{pg_embedding::PgEmbedding, IndexDistance, VectorChunk, VectorDBTS};

    use super::CreateIndexParams;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        // Create the sea-orm connection, s.t. we can share it with the application-level pool
        let database_url = "postgres://postgres:postgres@localhost/indexify";
        let db_conn = Database::connect(database_url).await.unwrap();
        let vector_db: VectorDBTS = Arc::new(PgEmbedding::new(
            crate::PgEmbeddingConfig {
                addr: database_url.to_string(),
                m: 3,
                efconstruction: 5,
                efsearch: 5,
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
        let vector_db: VectorDBTS = Arc::new(PgEmbedding::new(
            crate::PgEmbeddingConfig {
                addr: database_url.to_string(),
                m: 3,
                efconstruction: 5,
                efsearch: 5,
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
