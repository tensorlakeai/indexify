use std::{
    fmt::{self, Debug},
    iter::zip,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use arrow_array::{
    cast::as_string_array, types::Float32Type, FixedSizeListArray, PrimitiveArray, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use tracing;
use vectordb::{Connection, Table};

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};
use crate::server_config::LancedbConfig;

pub struct LanceDb {
    conn: Arc<dyn Connection>,
}

impl Debug for LanceDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceDb")
    }
}

impl LanceDb {
    pub async fn new(config: &LancedbConfig) -> Result<Self> {
        let conn = vectordb::connect(&config.path)
            .await
            .map_err(|e| anyhow!("unable to create db: {}", e))?;
        Ok(LanceDb { conn })
    }
}

#[async_trait]
impl VectorDb for LanceDb {
    fn name(&self) -> String {
        "lancedb".into()
    }

    #[tracing::instrument]
    async fn create_index(&self, index: CreateIndexParams) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    index.vector_dim as i32,
                ),
                true,
            ),
        ]));
        let batches = RecordBatchIterator::new(vec![], schema.clone());
        let _ = self
            .conn
            .create_table(&index.vectordb_index_name, Box::new(batches), None)
            .await
            .map_err(|e| anyhow!("unable to create table: {}", e))?;
        Ok(())
    }

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        let tbl = self
            .conn
            .open_table(index)
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;
        let ids = StringArray::from_iter_values(chunks.iter().map(|c| c.content_id.clone()));
        let vector_dim = chunks[0].embedding.len() as i32;
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            chunks
                .into_iter()
                .map(|c| Some(c.embedding.into_iter().map(Some))),
            vector_dim,
        );
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                tbl.schema().clone(),
                vec![Arc::new(ids), Arc::new(vectors)],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            tbl.schema().clone(),
        );

        let mut merge_insert = tbl.merge_insert(&["id"]);
        merge_insert
            .when_matched_update_all(None)
            .when_not_matched_insert_all();

        merge_insert
            .execute(Box::new(batches))
            .await
            .map_err(|e| anyhow!("unable to add to table {}", e))
    }

    #[tracing::instrument]
    #[tracing::instrument]
    async fn remove_embedding(&self, index: &str, content_id: &str) -> Result<()> {
        // Open the table
        let tbl = self
            .conn
            .open_table(index)
            .await
            .map_err(|e| anyhow!("unable to open table: {}", e))?;

        // Delete the rows where content_id is the key
        tbl.delete(&format!("id = {}", content_id))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to remove embeddings from lance db table for content_id {}: {}",
                    content_id,
                    e
                )
            })
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>> {
        let tbl: Arc<dyn Table> = self.conn.open_table(&index).await?;
        let query = &query_embedding[0..query_embedding.len()];
        let res = tbl
            .search(query)
            .column("vector")
            .limit(k as usize)
            .execute_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut results = vec![];
        for rb in &res {
            let ids = rb.column_by_name("id").unwrap();
            let id_values = as_string_array(&ids);
            let distances = rb.column_by_name("_distance").unwrap();
            let distance_values = distances
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()
                .unwrap();
            for (id, distance) in zip(id_values, distance_values) {
                results.push(SearchResult {
                    content_id: id.unwrap().to_string(),
                    confidence_score: distance.unwrap(),
                });
            }
        }

        Ok(results)
    }

    #[tracing::instrument]
    async fn drop_index(&self, index: String) -> Result<()> {
        self.conn
            .drop_table(&index)
            .await
            .map_err(|e| anyhow!("unable to drop index: {}", e))
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        let table = self
            .conn
            .open_table(index)
            .await
            .map_err(|e| anyhow!("unable to open table {} ", e))?;
        let rows = table.count_rows(None).await?;
        Ok(rows as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::vectordbs::{IndexDistance, VectorDBTS};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_search_basic() {
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: "hello-index".into(),
                vector_dim: 2,
                distance: crate::vectordbs::IndexDistance::Cosine,
                unique_params: None,
            })
            .await
            .unwrap();
        let chunk = VectorChunk {
            content_id: "id1".into(),
            embedding: vec![0., 2.],
        };
        let chunk1 = VectorChunk {
            content_id: "id2".into(),
            embedding: vec![0., 3.],
        };
        lance
            .add_embedding("hello-index", vec![chunk, chunk1])
            .await
            .unwrap();

        assert_eq!(lance.num_vectors("hello-index").await.unwrap(), 2);

        assert_eq!(
            lance
                .search("hello-index".to_string(), vec![0., 2.], 1)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_insertion_idempotent() {
        let index_name = "idempotent-index";
        let _ = std::fs::remove_dir_all("/tmp/lance.db/");
        let lance: VectorDBTS = Arc::new(
            LanceDb::new(&LancedbConfig {
                path: "/tmp/lance.db".to_string(),
            })
            .await
            .unwrap(),
        );
        lance
            .create_index(CreateIndexParams {
                vectordb_index_name: index_name.into(),
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
        lance
            .add_embedding(index_name, vec![chunk.clone()])
            .await
            .unwrap();
        lance.add_embedding(index_name, vec![chunk]).await.unwrap();
        let num_elements = lance.num_vectors(index_name).await.unwrap();

        assert_eq!(num_elements, 1);
    }
}
