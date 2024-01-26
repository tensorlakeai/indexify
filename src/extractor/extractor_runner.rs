use std::{collections::HashMap, str::FromStr};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use tracing::error;

use super::ExtractorTS;
use crate::{
    api,
    api::{ExtractorDescription, IndexDistance},
};

#[derive(Debug)]
pub struct ExtractorRunner {
    extractor: ExtractorTS,
}

impl ExtractorRunner {
    pub fn new(extractor: ExtractorTS) -> Self {
        Self { extractor }
    }

    pub fn matches_mime_type(
        &self,
        content: &internal_api::Content,
    ) -> Result<bool, anyhow::Error> {
        self.extractor.matches_mime_type(content)
    }

    pub fn extract(
        &self,
        content: Vec<internal_api::Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<internal_api::Content>>> {
        // the extractor runner filters by mime type to ensure that the extractor
        // supports the content mime type. If the extractor does not support the
        // content mime type, the content is filtered out.
        // to prevent this behavior, set the extractor input mime types to ["*/*"]
        let filtered_content = self.filter_extracted_content(content)?;

        let extracted_content = self.extractor.extract(filtered_content, input_params)?;
        Ok(extracted_content)
    }

    fn filter_extracted_content(
        &self,
        content: Vec<internal_api::Content>,
    ) -> Result<Vec<internal_api::Content>> {
        let filtered_content: Vec<internal_api::Content> = content
            .into_iter()
            .filter(|c| {
                match self.extractor.matches_mime_type(&c) {
                    Result::Ok(is_match) => is_match,
                    Result::Err(e) => {
                        error!("Failed to check mime type for content: {}", e);
                        false
                    }
                }
            })
            .collect();
        Ok(filtered_content)
    }

    pub fn extract_from_data(
        &self,
        data: Vec<u8>,
        mime: &str,
    ) -> Result<Vec<internal_api::Content>> {
        let content = internal_api::Content {
            bytes: data,
            mime: mime.to_string(),
            feature: None,
            labels: HashMap::new(),
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
                let distance = IndexDistance::from_str(&schema.distance).unwrap();
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
            name: extractor_schema.name.clone(),
            description: extractor_schema.description.clone(),
            input_params: extractor_schema.input_params,
            outputs,
            input_mime_types: extractor_schema.input_mimes,
        };
        Ok(extractor_description)
    }
}
