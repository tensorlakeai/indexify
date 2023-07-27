use anyhow::{anyhow, Result};
use entity::content::Model as ContentModel;
use std::sync::Arc;

use crate::{entity, index::IndexManager};

#[async_trait::async_trait]
pub trait Extractor<I, O> {
    async fn extract_and_store(
        &self,
        content: I,
        index_name: &str,
        repository: &str,
    ) -> Result<(), anyhow::Error>;

    fn description(&self) -> String;
}

pub struct EmbeddingExtractor {
    index_manager: Arc<IndexManager>,
}

impl EmbeddingExtractor {
    pub fn new(index_manager: Arc<IndexManager>) -> Self {
        Self { index_manager }
    }
}

#[async_trait::async_trait]
impl Extractor<ContentModel, Vec<f32>> for EmbeddingExtractor {
    async fn extract_and_store(
        &self,
        content: ContentModel,
        index_name: &str,
        repository: &str,
    ) -> Result<(), anyhow::Error> {
        let index = self
            .index_manager
            .load(repository, index_name)
            .await
            .map_err(|e| anyhow!("unable to load index: {}", e.to_string()))?;
        index
            .add_to_index(content)
            .await
            .map_err(|e| anyhow!("unable to add to index: {}", e.to_string()))?;
        Ok(())
    }

    fn description(&self) -> String {
        "Extractor to extract embeddings from content".to_string()
    }
}
