use std::{collections::HashMap, str::FromStr};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;

use super::ExtractorTS;
use indexify_api::{self as api, ExtractorDescription, ExtractorOutputSchema, IndexDistance};

#[derive(Debug)]
pub struct ExtractorRunner {
    extractor: ExtractorTS,
}

impl ExtractorRunner {
    pub fn new(extractor: ExtractorTS) -> Self {
        Self { extractor }
    }

    pub fn extract(
        &self,
        content: Vec<internal_api::Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<internal_api::Content>>> {
        let extracted_content = self.extractor.extract(content, input_params)?;
        Ok(extracted_content)
    }

    pub fn extract_from_data(
        &self,
        data: Vec<u8>,
        mime: &str,
    ) -> Result<Vec<internal_api::Content>> {
        let content = internal_api::Content {
            bytes: data,
            mime: mime.to_string(),
            features: vec![],
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
        let embedding_outputs: HashMap<String, ExtractorOutputSchema> = extractor_schema
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
        let metadata_outputs: HashMap<String, ExtractorOutputSchema> = extractor_schema
            .metadata_schemas
            .into_iter()
            .map(|(name, schema)| (name, api::ExtractorOutputSchema::Metadata(schema)))
            .collect();
        let outputs = embedding_outputs
            .into_iter()
            .chain(metadata_outputs)
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
