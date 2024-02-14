use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
use async_trait::async_trait;
use tracing;
use vectordb::Connection;

use super::{CreateIndexParams, SearchResult, VectorChunk, VectorDb};

#[derive(Debug)]
pub struct LanceDb {
    conn: Arc<dyn Connection>,
}

impl LanceDb {
    pub async fn new(path: &str) -> Result<Self> {
        let conn = vectordb::connect(path)
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
            Field::new("id", DataType::Int32, false),
            Field::new("vector", DataType::FixedSizeList(
              Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
          ]));

        Ok(())
    }j

    #[tracing::instrument]
    async fn add_embedding(&self, index: &str, chunks: Vec<VectorChunk>) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument]
    async fn search(
        &self,
        index: String,
        query_embedding: Vec<f32>,
        k: u64,
    ) -> Result<Vec<SearchResult>> {
        Ok(vec![])
    }

    #[tracing::instrument]
    async fn drop_index(&self, index: String) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument]
    async fn num_vectors(&self, index: &str) -> Result<u64> {
        Ok(0)
    }
}
