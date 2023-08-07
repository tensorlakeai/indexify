use anyhow::{anyhow, Ok, Result};

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc};
use tracing::info;

use crate::{
    persistence::{self, ExtractorConfig, ExtractorType},
    server_config,
    vectordbs::IndexDistance,
};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList, PyString},
};

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

#[pyclass]
struct PyContent {
    #[pyo3(get, set)]
    id: String,
    #[pyo3(get, set)]
    data: String,
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
    fn info(&self) -> Result<persistence::ExtractorConfig, anyhow::Error>;

    fn extract_embedding(
        &self,
        content: Vec<persistence::Content<String>>,
    ) -> Result<Vec<ExtractedEmbeddings>, anyhow::Error>;

    fn extract_embedding_query(&self, query: &str) -> Result<Vec<f32>, anyhow::Error>;

    fn extract_attributes(
        &self,
        content: Vec<persistence::Content<String>>,
    ) -> Result<Vec<AttributeData>, anyhow::Error>;
}

pub fn create_extractor(extractor_config: server_config::Extractor) -> Result<ExtractorTS> {
    match extractor_config.driver {
        server_config::ExtractorDriver::Python => {
            let extractor = PythonDriver::new(extractor_config.path)?;
            info!("extractor created: {:?}", extractor.info()?);
            Ok(Arc::new(extractor))
        }
        _ => Err(anyhow!("unsupported extractor driver")),
    }
}

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
    fn info(&self) -> Result<ExtractorConfig, anyhow::Error> {
        let info = Python::with_gil(|py| {
            let info = self.module_object.call_method0(py, "info")?;
            let extractor_info: ExtractorInfo = info.extract(py)?;
            let extractor_type = match extractor_info.output_datatype.as_str() {
                "embedding" => {
                    let embedding_schema: EmbeddingSchema =
                        info.getattr(py, "output_schema")?.extract(py)?;
                    Ok(ExtractorType::Embedding {
                        dim: embedding_schema.dim,
                        distance: IndexDistance::from_str(
                            embedding_schema.distance_metric.as_str(),
                        )?,
                    })
                }
                "attributes" => {
                    let schema: String = info.getattr(py, "output_schema")?.extract(py)?;
                    Ok(ExtractorType::Attributes { schema })
                }
                _ => Err(anyhow!("unsupported output datatype")),
            }?;
            let input_params =
                serde_json::from_str::<serde_json::Value>(&extractor_info.input_params)?;
            let extractor_config = ExtractorConfig {
                name: extractor_info.name,
                description: extractor_info.description,
                extractor_type,
                input_params,
            };
            Ok(extractor_config)
        })?;
        Ok(info)
    }

    fn extract_embedding(
        &self,
        content: Vec<persistence::Content<String>>,
    ) -> Result<Vec<ExtractedEmbeddings>, anyhow::Error> {
        let extracted_data = Python::with_gil(|py| {
            let kwargs = [
                ("overlap", 0.to_object(py)),
                ("text_splitter", "new_line".to_object(py)),
            ];
            let args = kwargs.into_py_dict(py);
            let content = content
                .into_iter()
                .map(|c| {
                    Py::new(
                        py,
                        PyContent {
                            id: c.id,
                            data: c.content,
                        },
                    )
                    .unwrap()
                })
                .collect::<Vec<Py<PyContent>>>();

            let extracted_data = self
                .module_object
                .call_method1(py, "extract", (content, args))?;
            let extracted_data: Vec<ExtractedEmbeddings> = extracted_data.extract(py)?;
            Ok(extracted_data)
        })?;
        Ok(extracted_data)
    }

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

    fn extract_attributes(
        &self,
        content: Vec<persistence::Content<String>>,
    ) -> Result<Vec<AttributeData>, anyhow::Error> {
        let extracted_data = Python::with_gil(|py| {
            let content = content.to_vec();
            let kwargs = [
                ("overlap", 0.to_object(py)),
                ("text_splitter", "new_line".to_object(py)),
            ];
            let args = kwargs.into_py_dict(py);
            let content = content
                .into_iter()
                .map(|c| {
                    Py::new(
                        py,
                        PyContent {
                            id: c.id,
                            data: c.content,
                        },
                    )
                    .unwrap()
                })
                .collect::<Vec<Py<PyContent>>>();

            let extracted_data = self
                .module_object
                .call_method1(py, "extract", (content, args))?;

            #[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
            struct InternalAttributeData {
                content_id: String,
                json: Option<String>,
            }
            let extracted_data: Vec<InternalAttributeData> = extracted_data.extract(py)?;
            let extracted_data = extracted_data
                .into_iter()
                .map(|attr| {
                    let json = if let Some(d) = attr.json.as_ref() {
                        let json_value: serde_json::Value = serde_json::from_str(d).unwrap();
                        Some(json_value)
                    } else {
                        None
                    };
                    AttributeData {
                        content_id: attr.content_id,
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
    use std::collections::HashMap;

    use crate::persistence::Content;

    use super::*;

    #[test]
    fn extract_embeddings() {
        let extractor =
            PythonDriver::new("indexify_extractors.embedding_extractor.MiniLML6Extractor".into())
                .unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "MiniLML6");
        assert_eq!(info.input_params, serde_json::json!({}));

        let content1 = Content::new("1".into(), "hello world".to_string(), HashMap::new());
        let content2 = Content::new(
            "2".into(),
            "indexify is awesome".to_string(),
            HashMap::new(),
        );

        let content = vec![content1, content2];
        let extracted_data = extractor.extract_embedding(content).unwrap();
        assert_eq!(extracted_data.len(), 2);
    }

    #[test]
    fn extract_embeddings_query() {
        let extractor =
            PythonDriver::new("indexify_extractors.embedding_extractor.MiniLML6Extractor".into())
                .unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "MiniLML6");

        let extracted_data = extractor.extract_embedding_query("hello world").unwrap();
        assert_eq!(extracted_data.len(), 384);
    }

    #[test]
    fn extract_attributes() {
        let extractor =
            PythonDriver::new("indexify_extractors.entity_extractor.EntityExtractor".into())
                .unwrap();

        let info = extractor.info().unwrap();
        assert_eq!(info.name, "EntityExtractor");

        let content1 = Content::new(
            "1".into(),
            "My name is Donald and I live in Seattle".to_string(),
            HashMap::new(),
        );
        let extracted_data = extractor.extract_attributes(vec![content1]).unwrap();
        assert_eq!(extracted_data.len(), 2);
    }
}
