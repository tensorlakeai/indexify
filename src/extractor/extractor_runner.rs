use std::str::FromStr;

use anyhow::{anyhow, Ok, Result};

use super::ExtractorTS;
use crate::{
    api,
    api::{ExtractorDescription, IndexDistance},
    internal_api::Content,
    server_config::ExtractorConfig,
};

#[derive(Debug)]
pub struct ExtractorRunner {
    extractor: ExtractorTS,
    config: ExtractorConfig,
}

impl ExtractorRunner {
    pub fn new(extractor: ExtractorTS, config: ExtractorConfig) -> Self {
        Self { extractor, config }
    }

    pub fn extract(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<Content>>> {
        let extracted_content = self.extractor.extract(content, input_params)?;
        Ok(extracted_content)
    }

    pub fn extract_from_data(&self, data: Vec<u8>, mime: &str) -> Result<Vec<Content>> {
        let content = Content {
            bytes: data,
            mime: mime.to_string(),
            feature: None,
        };
        let extracted_content = self.extract(vec![content], serde_json::Value::Null)?;
        let extracted_content = extracted_content
            .first()
            .ok_or_else(|| anyhow!("Expected one content item, got none"))?;
        Ok(extracted_content.to_owned())
    }

    pub fn info(&self) -> Result<ExtractorDescription> {
        let extractor_schema = self
            .extractor
            .schemas()
            .map_err(|e| anyhow!("Failed to get extractor schema: {}", e))?;
        let outputs = extractor_schema
            .embedding_schemas
            .into_iter()
            .map(|(name, schema)| {
                let distance = IndexDistance::from_str(&schema.distance_metric).unwrap();
                (
                    name,
                    api::ExtractorOutputSchema::Embedding(api::EmbeddingSchema {
                        dim: schema.dim,
                        distance,
                    }),
                )
            })
            .collect();
        let extractor_description = ExtractorDescription {
            name: self.config.name.clone(),
            description: self.config.description.clone(),
            input_params: extractor_schema.input_params,
            outputs,
        };
        Ok(extractor_description)
    }
}
