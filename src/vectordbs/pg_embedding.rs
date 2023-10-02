use async_trait::async_trait;
use sea_orm::{query::JsonValue, ConnectionTrait, ExecResult, FromQueryResult};
use serde_json::Value;
use tracing::warn;

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb, VectorDbError};
use crate::PgEmbeddingConfig;
use itertools::Itertools;
use sea_orm::{self, DbBackend, DbConn, Statement};

/// PgEmbeddingDb
pub struct PgEmbedding {
    config: PgEmbeddingConfig,
    db_conn: DbConn,
}

impl PgEmbedding {
    pub fn new(config: PgEmbeddingConfig, db_conn: DbConn) -> PgEmbedding {
        Self { config, db_conn }
    }

    fn escape_to_valid_indexname(x: String) -> String {
        x.replace('-', "_")
    }
}

/// Acts as a salt, to make sure there are no collisions with other tables
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
        let index_name = Self::escape_to_valid_indexname(index_name);
        let vector_dim = index.vector_dim;
        let distance_extension = match &index.distance {
            crate::vectordbs::IndexDistance::Euclidean => "",
            crate::vectordbs::IndexDistance::Cosine => "ann_cos_ops",
            crate::vectordbs::IndexDistance::Dot => "ann_manhattan_ops",
        };

        let query = format!(r#"CREATE EXTENSION IF NOT EXISTS EMBEDDING;"#);
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        // If more than one row is affected, something funky has occurred
        if exec_res.rows_affected() > 0 {
            return Err(VectorDbError::IndexCreationError(format!(
                "More than one table affected! {:?} {:?}",
                index_name.clone(),
                exec_res.rows_affected()
            )));
        };

        // the "id" here corresponds to the chunk-id, and is NOT SERIAL
        let query = format!(
            r#"CREATE TABLE IF NOT EXISTS {INDEX_TABLE_PREFIX}{index_name}(chunk_id VARCHAR(1024) PRIMARY KEY , embedding real[]);"#
        );
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        // If more than one row is affected, something funky has occurred
        if exec_res.rows_affected() > 1 {
            return Err(VectorDbError::IndexCreationError(format!(
                "More than one table affected! {:?} {:?}",
                index_name.clone(),
                exec_res.rows_affected()
            )));
        };
        let query = format!(
            r#"
                CREATE INDEX ON {INDEX_TABLE_PREFIX}{index_name} USING hnsw(embedding {distance_extension}) WITH (dims={vector_dim}, m={}, efconstruction={}, efsearch={});
            "#,
            self.config.m, self.config.efconstruction, self.config.efsearch
        );
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        // If more than one row is affected, something funky has occurred
        if exec_res.rows_affected() > 1 {
            return Err(VectorDbError::IndexCreationError(format!(
                "More than one table affected! {:?} {:?}",
                index_name.clone(),
                exec_res.rows_affected()
            )));
        };
        let query = format!(
            r#"SET enable_seqscan = off;"#,
            // The "enable seqscan = off" should not be set for the whole server. Will need to put this out
        );
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_string(DbBackend::Postgres, query))
            .await
            .map_err(|e| {
                VectorDbError::IndexCreationError(format!("{:?}: {:?}", index_name.clone(), e))
            })?;
        // If more than one row is affected, something funky has occurred
        if exec_res.rows_affected() > 0 {
            return Err(VectorDbError::IndexCreationError(format!(
                "More than one table affected! {:?} {:?}",
                index_name.clone(),
                exec_res.rows_affected()
            )));
        };
        // Make a SELECT to make sure that the table exists (i.e. there was no insertion error)
        let query = format!(
            r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = $1
                ) AS table_existence;
            "#
        );
        warn!("Running query: {:?}", query);
        let response: JsonValue = JsonValue::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            query.as_str(),
            [sea_orm::Value::String(Some(Box::new(format!(
                "{INDEX_TABLE_PREFIX}{index_name}"
            ))))],
        ))
        .one(&self.db_conn)
        .await
        .map_err(|e| {
            VectorDbError::IndexReadError(format!(
                "Checking if table was created {:?}: {:?}",
                index_name, e
            ))
        })?
        .ok_or(VectorDbError::IndexReadError(
            "SELECT EXISTS did not run successfully".to_string(),
        ))?;
        warn!("Return is: {:?}", response);
        match response {
            Value::Object(mut x) => {
                let x = x
                    .remove("table_existence")
                    .ok_or(VectorDbError::IndexReadError(
                        "SELECT EXISTS did not return a dictionary with key 'table_existence'"
                            .to_string(),
                    ))?;
                match x {
                    Value::Bool(x) => {
                        if x {
                            Ok(())
                        } else {
                            Err(VectorDbError::IndexCreationError(format!(
                            "Could not verify that table exists when creationg code is called!! {:?} {:?}",
                            index_name.clone(),
                            x
                        )))
                        }
                    }
                    _ => Err(VectorDbError::IndexReadError(
                        "SELECT EXISTS did not return boolean inside the object".to_string(),
                    )),
                }
            }
            _ => Err(VectorDbError::IndexReadError(
                "SELECT EXISTS did not an object".to_string(),
            )),
        }
    }

    async fn add_embedding(
        &self,
        index: &str,
        chunks: Vec<VectorChunk>,
    ) -> Result<(), VectorDbError> {
        let index = Self::escape_to_valid_indexname(index.to_string());
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
        warn!("Insert query: {:?}", query);
        // Due to the limitations of sea-query (we cannot encode tuples, nor can we encode arrays of arrays)
        // We iteratively build out the query manually
        let parameters = chunks
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
        let index = Self::escape_to_valid_indexname(index);
        // TODO: How to turn query_embedding into an array[...]
        // TODO: confidence_score is a distance here, let's make sure that similarity / distance is the same across vectors databases
        let query = format!(
            r#"
            SELECT chunk_id, embedding <-> $1 AS confidence_score FROM {INDEX_TABLE_PREFIX}{index} ORDER BY embedding <-> $1 LIMIT {k};
        "#
        );
        warn!("Query is {:?}", query);
        let query_embedding = query_embedding
            .into_iter()
            .map(|x| sea_orm::Value::Float(Some(x)))
            .collect();
        SearchResult::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            query.as_str(),
            [sea_orm::Value::Array(
                sea_query::ArrayType::Float,
                Some(Box::new(query_embedding)),
            )],
        ))
        .all(&self.db_conn)
        .await
        .map_err(|e| VectorDbError::IndexReadError(format!("Search Error {:?}: {:?}", index, e)))
    }

    // TODO: Should change index to &str
    async fn drop_index(&self, index: String) -> Result<(), VectorDbError> {
        let index = Self::escape_to_valid_indexname(index);
        let query = format!("DROP TABLE IF EXISTS {INDEX_TABLE_PREFIX}{index};");
        warn!("drop query is: {:?}", query);
        let exec_res: ExecResult = self
            .db_conn
            .execute(Statement::from_string(
                DbBackend::Postgres,
                query.to_string(),
            ))
            .await
            .map_err(|e| VectorDbError::IndexDeletionError(index.clone(), format!("{:?}", e)))?;
        if exec_res.rows_affected() <= 1 {
            // If none was deleted, then that's also ok!
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
        let index = Self::escape_to_valid_indexname(index.to_string());
        let query = format!("SELECT COUNT(*) FROM TABLE {INDEX_TABLE_PREFIX}{index};");
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
