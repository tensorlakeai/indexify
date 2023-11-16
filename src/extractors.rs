use anyhow::{anyhow, Ok};
use pyo3::prelude::*;

use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use tracing::info;

use crate::{
    content_reader::ContentReader,
    internal_api::{self, ContentPayload, ExtractedContent},
    server_config,
};
use pyo3::types::{PyList, PyString};

const EXTRACT_METHOD: &str = "extract";

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub embedding_schemas: HashMap<String, EmbeddingSchema>,
    pub input_params: serde_json::Value,
}

#[derive(Clone)]
#[pyclass]
pub struct PyFeature {
    #[pyo3(get, set)]
    pub feature_type: String,
    #[pyo3(get, set)]
    pub name: String,
    #[pyo3(get, set)]
    pub value: String,
}

#[derive(Clone)]
#[pyclass]
pub struct PyContent {
    #[pyo3(get, set)]
    pub content_type: String,
    #[pyo3(get, set)]
    pub data: Vec<u8>,
    #[pyo3(get, set)]
    pub feature: Option<PyFeature>,
}

impl TryFrom<PyContent> for internal_api::ExtractedContent {
    type Error = anyhow::Error;

    fn try_from(py_content: PyContent) -> Result<Self, Self::Error> {
        let content_type = match py_content.content_type.as_str() {
            "text" => Ok(internal_api::ContentType::Text),
            "audio" => Ok(internal_api::ContentType::Audio),
            "image" => Ok(internal_api::ContentType::Image),
            _ => Err(anyhow!(format!(
                "unknown content type: {}",
                py_content.content_type
            ))),
        }?;
        let feature = match py_content.feature {
            Some(py_feature) => {
                let feature_type = match py_feature.feature_type.as_str() {
                    "embedding" => internal_api::FeatureType::Embedding,
                    "named_entity" => internal_api::FeatureType::NamedEntity,
                    "metadata" => internal_api::FeatureType::Metadata,
                    _ => internal_api::FeatureType::Unknown,
                };
                let data = serde_json::from_str(&py_feature.value)?;
                let name = py_feature.name;
                Some(internal_api::Feature {
                    feature_type,
                    name,
                    data,
                })
            }
            None => None,
        };
        let extracted_content = internal_api::ExtractedContent {
            content_type,
            source: py_content.data,
            feature,
        };
        Ok(extracted_content)
    }
}

impl TryFrom<internal_api::ExtractedContent> for PyContent {
    type Error = anyhow::Error;

    fn try_from(content: internal_api::ExtractedContent) -> Result<Self, Self::Error> {
        let content_type = content.content_type.to_string();
        let data = content.source;
        Ok(PyContent {
            content_type,
            data,
            feature: None,
        })
    }
}

impl PyContent {
    pub async fn form_content_payload(
        content_payload: ContentPayload,
    ) -> Result<Self, anyhow::Error> {
        let content_type = content_payload.content_type.to_string();
        let content_reader = ContentReader::new(content_payload);
        let data = content_reader.read().await?;
        Ok(Self {
            content_type,
            data,
            feature: None,
        })
    }

    pub fn new(data: String) -> Self {
        let content_type = internal_api::ContentType::Text.to_string();
        Self {
            content_type,
            data: data.into_bytes().to_vec(),
            feature: None,
        }
    }

    pub fn from_bytes(data: Vec<u8>, content_type: internal_api::ContentType) -> Self {
        Self {
            content_type: content_type.to_string(),
            data,
            feature: None,
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

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtractorInfo {
    pub output_schema: serde_json::Value,
    pub input_params: serde_json::Value,
}

pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;
pub trait Extractor {
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error>;

    fn extract(
        &self,
        content: Vec<ExtractedContent>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<ExtractedContent>>, anyhow::Error>;

    fn extract_embedding_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error>;
}

#[tracing::instrument]
pub fn create_extractor(
    extractor_config: Arc<server_config::ExtractorConfig>,
) -> Result<ExtractorTS, anyhow::Error> {
    let extractor = PythonExtractor::new(extractor_config.module.clone())?;
    info!("extractor created: {}", extractor_config.name.clone());
    Ok(Arc::new(extractor))
}

#[derive(Debug)]
pub struct PythonExtractor {
    extractor_wrapper: PyObject,
}

impl PythonExtractor {
    pub fn new(module_name: String) -> Result<Self, anyhow::Error> {
        let extractor_wrapper = Python::with_gil(|py| {
            let syspath: &PyList = py
                .import("sys")?
                .getattr("path")?
                .downcast()
                .map_err(|e| anyhow!(e.to_string()))?;
            syspath.insert(0, ".")?;
            let module = PyModule::import(
                py,
                PyString::new(py, "indexify_extractor_sdk.base_extractor"),
            )?;
            let extractor_wrapper_class = module.getattr(PyString::new(py, "ExtractorWrapper"))?;
            let entry_point = PyString::new(py, &module_name);

            let extractor_wrapper = extractor_wrapper_class.call1((entry_point,))?.into_py(py);
            Ok(extractor_wrapper)
        })?;
        Ok(Self { extractor_wrapper })
    }
}

#[async_trait::async_trait]
impl Extractor for PythonExtractor {
    #[tracing::instrument]
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error> {
        let info = Python::with_gil(|py| {
            let info = self.extractor_wrapper.call_method0(py, "schemas")?;
            let input_params: String = info.getattr(py, "input_params")?.extract(py)?;
            let input_params = serde_json::from_str(&input_params)?;
            let embedding_schemas: HashMap<String, EmbeddingSchema> = info
                .getattr(py, "embedding_schemas")?
                .extract(py)
                .map_err(|e| anyhow!(e.to_string()))?;
            Ok(ExtractorSchema {
                embedding_schemas,
                input_params,
            })
        })?;
        Ok(info)
    }

    fn extract(
        &self,
        content: Vec<ExtractedContent>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<ExtractedContent>>, anyhow::Error> {
        let extracted_content = Python::with_gil(|py| {
            let json_string = serde_json::to_string(&input_params)?.into_py(py);
            let content: Vec<PyContent> = content
                .into_iter()
                .map(PyContent::try_from)
                .collect::<Result<Vec<PyContent>, anyhow::Error>>()?;

            let extracted_data = self
                .extractor_wrapper
                .call_method1(py, EXTRACT_METHOD, (content, json_string))
                .map_err(|e| {
                    let trace_back = e.traceback(py).map(|t| t.format());
                    anyhow!(
                        "error calling extract method: err {}, trace back: {:?}",
                        e.to_string(),
                        trace_back.unwrap().unwrap()
                    )
                })?;
            let py_extracted_data: Vec<Vec<PyObject>> = extracted_data.clone().extract(py).unwrap();
            let mut extracted_content = Vec::new();
            for (_, list1) in py_extracted_data.iter().enumerate() {
                let mut temp = Vec::new();
                for (_, py_content) in list1.iter().enumerate() {
                    let content_type: String =
                        py_content.getattr(py, "content_type")?.extract(py)?;
                    let data: Vec<u8> = py_content.getattr(py, "data")?.extract(py)?;
                    let py_feature: Option<PyObject> =
                        py_content.getattr(py, "feature")?.extract(py)?;
                    let feature = match py_feature {
                        Some(py_feature) => {
                            let feature_type: String =
                                py_feature.getattr(py, "feature_type")?.extract(py)?;
                            let feature_type = internal_api::FeatureType::from_str(&feature_type)?;
                            let name: String = py_feature.getattr(py, "name")?.extract(py)?;
                            let value: String = py_feature.getattr(py, "value")?.extract(py)?;
                            let value: serde_json::Value = serde_json::from_str(&value)?;
                            Some(internal_api::Feature {
                                feature_type,
                                name,
                                data: value,
                            })
                        }
                        None => None,
                    };
                    temp.push(ExtractedContent {
                        content_type: content_type.parse::<internal_api::ContentType>()?,
                        source: data,
                        feature,
                    });
                }
                extracted_content.push(temp);
            }
            Ok(extracted_content)
        })?;

        Ok(extracted_content)
    }

    fn extract_embedding_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error> {
        let extracted_data: Vec<f32> = Python::with_gil(|py| {
            let extracted_data = self
                .extractor_wrapper
                .call_method1(py, "extract_query_embeddings", (query,))
                .map_err(|e| anyhow!(e.to_string()))?;
            let embeddings: Vec<f32> = extracted_data
                .extract(py)
                .map_err(|e| anyhow!(e.to_string()))?;
            Ok(embeddings)
        })
        .map_err(|e| anyhow!(e.to_string()))?;
        Ok(extracted_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_content() {
        let content1 = PyContent::new("My name is Donald and I live in Seattle".to_string())
            .try_into()
            .unwrap();
        let content2 = PyContent::new("My name is Donald and I live in Seattle".to_string())
            .try_into()
            .unwrap();
        let content = vec![content1, content2];

        let extractor =
            PythonExtractor::new("indexify_extractor_sdk.mock_extractor:MockExtractor".into())
                .unwrap();

        let input_params = serde_json::from_str("{\"a\":5,\"b\":\"recursive\"}").unwrap();

        let extracted_data = extractor.extract(content.clone(), input_params).unwrap();
        assert_eq!(extracted_data.len(), 1);
        assert_eq!(extracted_data.get(0).unwrap().len(), 3);
        assert_eq!(
            extracted_data.get(0).unwrap().get(0).unwrap().content_type,
            internal_api::ContentType::Text
        );

        // Pass in empty input params

        let extracted_data1 = extractor.extract(content, json!({})).unwrap();
        assert_eq!(extracted_data1.len(), 1);
    }

    #[test]
    fn extract_embedding() {
        let extractor =
            PythonExtractor::new("minilm_l6_embedding:MiniLML6Extractor".into()).unwrap();

        let content1 = PyContent::new("My name is Donald and I live in Seattle".to_string())
            .try_into()
            .unwrap();
        let content2 = PyContent::new("My name is Donald and I live in Seattle".to_string())
            .try_into()
            .unwrap();
        let content = vec![content1, content2];
        let extracted_data1 = extractor.extract(content, json!({})).unwrap();
        assert_eq!(extracted_data1.len(), 2);
    }

    #[test]
    fn extract_from_blob() {
        let extractor = PythonExtractor::new(
            "indexify_extractor_sdk.mock_extractor:MockExtractorNoInputParams".into(),
        )
        .unwrap();

        let data = std::fs::read("extractors_tests/data/test.pdf").unwrap();
        let content = PyContent::from_bytes(data, internal_api::ContentType::Text)
            .try_into()
            .unwrap();
        let extracted_data = extractor.extract(vec![content], json!({})).unwrap();

        assert_eq!(extracted_data.len(), 1);
        //assert_eq!(extracted_data[0].embeddings.len(), 384);
    }

    #[test]
    fn extract_index_schema() {
        let extractor =
            PythonExtractor::new("indexify_extractor_sdk.mock_extractor:MockExtractor".into())
                .unwrap();

        let extractor_schema = extractor.schemas().unwrap();

        assert_ne!(extractor_schema.input_params, json!({}));
    }
}
