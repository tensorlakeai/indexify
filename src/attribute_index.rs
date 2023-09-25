use anyhow::Result;

use std::sync::Arc;

use crate::persistence::{ExtractedAttributes, ExtractorConfig, Repository};

pub struct AttributeIndexManager {
    repository: Arc<Repository>,
}

impl AttributeIndexManager {
    pub fn new(repository: Arc<Repository>) -> Self {
        Self { repository }
    }

    pub async fn create_index(
        &self,
        repository: &str,
        index_name: &str,
        extractor_config: ExtractorConfig,
    ) -> Result<()> {
        // TODO: create a new table for the index from a postgres schema
        self.repository
            .create_index_metadata(
                repository,
                &extractor_config.name,
                index_name,
                "structured_store",
                serde_json::json!(extractor_config.output_schema),
                "structured_index",
            )
            .await?;
        Ok(())
    }

    pub async fn add_index(
        &self,
        repository: &str,
        index_name: &str,
        extracted_attributes: ExtractedAttributes,
    ) -> Result<()> {
        self.repository
            .add_attributes(repository, index_name, extracted_attributes)
            .await?;
        Ok(())
    }

    pub async fn get_attributes(
        &self,
        repository: &str,
        index_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedAttributes>> {
        let extracted_attributes = self
            .repository
            .get_extracted_attributes(repository, index_name, content_id)
            .await?;
        Ok(extracted_attributes)
    }
}
