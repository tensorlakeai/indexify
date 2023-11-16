use anyhow::{anyhow, Ok, Result};

use pythonize::pythonize;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::{
    content_reader::ContentReader,
    internal_api::ContentPayload,
    persistence::{self, ExtractorDescription},
    server_config,
};
use pyo3::{
    prelude::*,
    types::{PyList, PyString},
};

const EXTRACT_METHOD: &str = "_extract";

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct Content {
    #[pyo3(get, set)]
    pub id: String,
    #[pyo3(get, set)]
    pub content_type: String,
    #[pyo3(get, set)]
    pub data: Vec<u8>,
}

impl Content {
    pub async fn form_content_payload(
        content_id: String,
        content_payload: ContentPayload,
    ) -> Result<Self> {
        let content_type = content_payload.content_type.to_string();
        let content_reader = ContentReader::new(content_payload);
        let data = content_reader.read().await?;
        Ok(Self {
            id: content_id,
            content_type,
            data,
        })
    }

    pub fn new(id: String, data: String) -> Self {
        Content {
            id,
            content_type: "text".to_string(),
            data: data.into_bytes().to_vec(),
        }
    }

    pub fn from_bytes(id: String, data: Vec<u8>, content_type: &str) -> Self {
        Content {
            id,
            content_type: content_type.to_string(),
            data,
        }
    }
}

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

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct ExtractorInfo {
    pub name: String,
    pub description: String,
    pub output_datatype: String,
    pub input_params: String,
}

pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;
pub trait Extractor {
    fn info(&self) -> Result<persistence::ExtractorDescription, anyhow::Error>;

    fn extract_embedding(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<ExtractedEmbeddings>, anyhow::Error>;

    fn extract_embedding_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error>;

    fn extract_attributes(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<AttributeData>, anyhow::Error>;
}

#[tracing::instrument]
pub fn create_extractor(
    extractor_config: Arc<server_config::ExtractorConfig>,
) -> Result<ExtractorTS> {
    let extractor = PythonDriver::new(extractor_config.module.clone())?;
    info!("extractor created: {:?}", extractor.info()?.name);
    Ok(Arc::new(extractor))
}

#[derive(Debug)]
pub struct PythonDriver {
    module_object: PyObject,
}

impl PythonDriver {
    pub fn new(module_name: String) -> Result<Self> {
        let (module, class_name) = Self::split_module_class(&module_name)
            .ok_or_else(|| anyhow!("invalid module name: {}", module_name))?;

        let module = Python::with_gil(|py| {
            let syspath: &PyList = py
                .import("sys")?
                .getattr("path")?
                .downcast()
                .map_err(|e| anyhow!(e.to_string()))?;
            syspath.insert(0, ".")?;
            let module = PyModule::import(py, PyString::new(py, &module))?;
            let dpr_class = module.getattr(PyString::new(py, &class_name))?;
            let dpr_object = dpr_class.call0()?.into_py(py);
            Ok(dpr_object)
        })?;
        Ok(Self {
            module_object: module,
        })
    }

    pub fn split_module_class(name: &str) -> Option<(String, String)> {
        let re = Regex::new(r"(?s)(.*)\.(.*)$").unwrap();
        re.captures(name).and_then(|cap| {
            let module_name = cap.get(1).map(|m| m.as_str());
            let class_name = cap.get(2).map(|c| c.as_str());
            match (module_name, class_name) {
                (Some(m), Some(c)) => Some((m.into(), c.into())),
                _ => None,
            }
        })
    }
}

#[async_trait::async_trait]
impl Extractor for PythonDriver {
    #[tracing::instrument]
    fn info(&self) -> Result<ExtractorDescription, anyhow::Error> {
        let info = Python::with_gil(|py| {
            let info = self.module_object.call_method0(py, "_info")?;
            let extractor_info: String = info.extract(py)?;
            let extractor_config: ExtractorDescription = serde_json::from_str(&extractor_info)?;
            Ok(extractor_config)
        })?;
        Ok(info)
    }

    #[tracing::instrument]
    fn extract_embedding(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<ExtractedEmbeddings>, anyhow::Error> {
        let extracted_data = Python::with_gil(|py| {
            let kwargs = pythonize(py, &input_params)?;
            let extracted_data =
                self.module_object
                    .call_method1(py, EXTRACT_METHOD, (content, kwargs))?;
            let extracted_data: Vec<ExtractedEmbeddings> = extracted_data.extract(py)?;
            Ok(extracted_data)
        })?;
        Ok(extracted_data)
    }

    #[tracing::instrument]
    fn extract_embedding_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error> {
        let extracted_data = Python::with_gil(|py| {
            let extracted_data =
                self.module_object
                    .call_method1(py, "extract_query_embeddings", (query,))?;
            let embeddings: Vec<f32> = extracted_data.extract(py)?;
            Ok(embeddings)
        })?;
        Ok(extracted_data)
    }

    #[tracing::instrument]
    fn extract_attributes(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<AttributeData>, anyhow::Error> {
        let extracted_data = Python::with_gil(|py| {
            let kwargs = pythonize(py, &input_params)?;
            let extracted_data = self
                .module_object
                .call_method1(py, EXTRACT_METHOD, (content, kwargs))
                .unwrap();

            #[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
            struct InternalAttributeData {
                content_id: String,
                text: String,
                attributes: Option<String>,
            }
            let extracted_data: Vec<InternalAttributeData> = extracted_data.extract(py).unwrap();
            let extracted_data = extracted_data
                .into_iter()
                .map(|attr| {
                    let json = if let Some(d) = attr.attributes.as_ref() {
                        let json_value: serde_json::Value = serde_json::from_str(d).unwrap();
                        Some(json_value)
                    } else {
                        None
                    };
                    AttributeData {
                        content_id: attr.content_id,
                        text: attr.text,
                        json,
                    }
                })
                .collect();
            Ok(extracted_data)
        })?;
        Ok(extracted_data)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extract_embeddings() {
        let extractor = PythonDriver::new("identity_hash_embedding.IdentityHashEmbedding".into()).unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "IdentityHashEmbedding");
        let json_schema = "{\"properties\":{\"overlap\":{\"default\":0,\"title\":\"Overlap\",\"type\":\"integer\"},\"text_splitter\":{\"default\":\"recursive\",\"enum\":[\"char\",\"recursive\"],\"title\":\"Text Splitter\",\"type\":\"string\"}},\"title\":\"EmbeddingInputParams\",\"type\":\"object\"}";
        assert_eq!(info.input_params.to_string(), json_schema);

        let content1 = Content::new("1".into(), "hello world".to_string());
        let content2 = Content::new("2".into(), "indexify is awesome".to_string());

        let content = vec![content1, content2];
        let input_params =
            serde_json::from_str("{\"overlap\":5,\"text_splitter\":\"recursive\"}").unwrap();
        let extracted_data = extractor.extract_embedding(content, input_params).unwrap();
        assert_eq!(extracted_data.len(), 2);
    }

    #[test]
    fn extract_embeddings_query() {
        let extractor = PythonDriver::new("identity_hash_embedding.IdentityHashEmbedding".into()).unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "IdentityHashEmbedding");

        let extracted_data = extractor.extract_embedding_query("hello world").unwrap();
        assert_eq!(extracted_data.len(), 32);
    }

    #[test]
    fn extract_attributes() {
        let extractor = PythonDriver::new("language_extractor.LanguageExtractor".into()).unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "LanguageExtractor");

        let content1 = Content::new(
            "1".into(),
            "My name is Donald and I live in Seattle".to_string(),
        );
        let extracted_data = extractor
            .extract_attributes(vec![content1], json!({}))
            .unwrap();
        assert_eq!(extracted_data.len(), 1);
    }

    #[test]
    fn extract_from_blob() {
        let extractor = PythonDriver::new("pdf_embedder.PDFEmbedder".into()).unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "PDFEmbedder");

        let data = std::fs::read("extractors_tests/data/test.pdf").unwrap();
        let content = Content::from_bytes("1".into(), data, "pdf");
        println!("Finished creating Content");
        let extracted_data = extractor
            .extract_embedding(vec![content], json!({}))
            .unwrap();
        println!("Hello");

        assert_eq!(extracted_data.len(), 28);
        assert_eq!(extracted_data[0].embeddings.len(), 384);
    }
}
