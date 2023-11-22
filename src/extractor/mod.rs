use anyhow::{anyhow, Ok};
use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::info;

use crate::{
    extractor::extractors::{PyContent, PythonExtractor},
    internal_api::{self, ExtractedContent},
    server_config::ExtractorConfig,
};

pub mod extractors;
mod python_path;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

pub trait Extractor {
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error>;

    fn extract(
        &self,
        content: Vec<ExtractedContent>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<ExtractedContent>>, anyhow::Error>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub embedding_schemas: HashMap<String, EmbeddingSchema>,
    pub input_params: serde_json::Value,
}
pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub text: String,
    pub embeddings: Vec<f32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct AttributeData {
    pub content_id: String,
    pub text: String,
    pub json: Option<serde_json::Value>,
}

#[tracing::instrument]
pub fn create_extractor(extractor_path: &str, name: &str) -> Result<ExtractorTS, anyhow::Error> {
    let tokens: Vec<&str> = extractor_path.split(':').collect();
    if tokens.len() != 2 {
        return Err(anyhow!("invalid extractor path: {}", extractor_path));
    }
    let module_name = tokens[0].trim_end_matches(".py").replace('/', ".");
    let class_name = tokens[1].trim();
    let extractor = PythonExtractor::new(&module_name, class_name)?;
    info!(
        "extractor created: name: {}, python module: {}, class name: {}",
        name, module_name, class_name
    );
    Ok(Arc::new(extractor))
}

pub fn run_extractor(
    extractor_path: Option<String>,
    text: Option<String>,
    file_path: Option<String>,
) -> Result<Vec<ExtractedContent>, anyhow::Error> {
    let extractor_path = match extractor_path {
        Some(extractor_path) => Ok(extractor_path),
        None => {
            info!("no module name provided, looking up indexify.yaml");
            let extractor_config = ExtractorConfig::from_path("indexify.yaml".into())?;
            Ok(extractor_config.module)
        }
    }?;
    info!("looking up extractor at path: {}", &extractor_path);
    python_path::set_python_path(&extractor_path)?;
    let extractor = create_extractor(&extractor_path, &extractor_path)?;
    
    match (text, file_path) {
        (Some(text), None) => {
            let content = PyContent::new(text).try_into()?;
            let extracted_content = extractor.extract(vec![content], json!({}))?;
            let content = extracted_content
                .get(0)
                .ok_or(anyhow!("no content was extracted"))?
                .to_owned();
            Ok(content)
        }
        (None, Some(file_path)) => {
            let data = std::fs::read(file_path)?;
            let content =
                PyContent::from_bytes(data, internal_api::ContentType::Text).try_into()?;
            let extracted_content = extractor.extract(vec![content], json!({}))?;
            let content = extracted_content
                .get(0)
                .ok_or(anyhow!("no content was extracted"))?
                .to_owned();
            Ok(content)
        }
        _ => Err(anyhow!("either text or file path must be provided")),
    }
}
