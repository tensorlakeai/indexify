use std::{collections::HashMap, path::Path, str::FromStr};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use pyo3::{
    prelude::*,
    types::{PyList, PyString},
};

use super::{EmbeddingSchema, Extractor, ExtractorSchema};

const EXTRACT_METHOD: &str = "extract";

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
    #[pyo3(get, set)]
    pub labels: Option<HashMap<String, String>>,
}

impl TryFrom<PyContent> for internal_api::Content {
    type Error = anyhow::Error;

    fn try_from(py_content: PyContent) -> Result<Self, Self::Error> {
        let mime_type = mime::Mime::from_str(&py_content.content_type)?;
        let feature = match py_content.feature {
            Some(py_feature) => {
                let feature_type = match py_feature.feature_type.as_str() {
                    "embedding" => internal_api::FeatureType::Embedding,
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
        let extracted_content = internal_api::Content {
            mime: mime_type.to_string(),
            bytes: py_content.data,
            feature,
            labels: py_content.labels.unwrap_or_default(),
        };
        Ok(extracted_content)
    }
}

impl TryFrom<internal_api::Content> for PyContent {
    type Error = anyhow::Error;

    fn try_from(content: internal_api::Content) -> Result<Self, Self::Error> {
        let content_type = content.mime.to_string();
        let data = content.bytes;
        Ok(PyContent {
            content_type,
            data,
            feature: None,
            labels: None,
        })
    }
}

impl PyContent {
    pub fn new(data: String, labels: Option<HashMap<String, String>>) -> Self {
        Self {
            content_type: mime::TEXT_PLAIN.to_string(),
            data: data.into_bytes().to_vec(),
            feature: None,
            labels,
        }
    }

    pub fn from_bytes(data: Vec<u8>, content_type: mime::Mime) -> Self {
        Self {
            content_type: content_type.to_string(),
            data,
            feature: None,
            labels: None,
        }
    }
}

#[derive(Debug)]
pub struct PythonExtractor {
    extractor_wrapper: PyObject,
    extractor_schema: ExtractorSchema,
}

impl PythonExtractor {
    pub fn new_from_extractor_path(extractor_path: &str) -> Result<Self> {
        let tokens: Vec<&str> = extractor_path.split(':').collect();
        if tokens.len() != 2 {
            return Err(anyhow!("invalid extractor path: {}", extractor_path));
        }
        let module_path = tokens[0];
        let class_name = tokens[1].trim();
        let module_file_name = Path::new(module_path)
            .file_name()
            .ok_or(anyhow!("couldn't find model file name: {:?}", module_path))?
            .to_str()
            .ok_or(anyhow!("couldn't find model file name: {:?}", module_path))?;

        let module_name = module_file_name.trim_end_matches(".py");
        Self::new(module_name, class_name)
    }

    pub fn new(module_name: &str, class_name: &str) -> Result<Self, anyhow::Error> {
        let (extractor_wrapper, extractor_schema) = Python::with_gil(|py| {
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
            let entry_point = module_name.to_owned().into_py(py);
            let class_name = class_name.to_owned().into_py(py);

            let extractor_wrapper = extractor_wrapper_class
                .call1((entry_point.clone(), class_name.clone()))?
                .into_py(py);
            let description = extractor_wrapper.call_method0(py, "describe")?.into_py(py);
            let input_params: String = description.getattr(py, "input_params")?.extract(py)?;
            let input_mimes: Vec<String> =
                description.getattr(py, "input_mime_types")?.extract(py)?;
            let input_params = serde_json::from_str(&input_params)?;
            let metadata_schemas_temp: HashMap<String, String> = description
                .getattr(py, "metadata_schemas")?
                .extract(py)
                .map_err(|e| anyhow!(e.to_string()))?;
            let mut metadata_schemas = HashMap::new();
            for (key, value) in metadata_schemas_temp.iter() {
                println!("key: {}, value: {}", key, value);
                let value: serde_json::Value = serde_json::from_str(&value)?;
                metadata_schemas.insert(key.clone(), value);
            }
            let embedding_schemas: HashMap<String, EmbeddingSchema> = description
                .getattr(py, "embedding_schemas")?
                .extract(py)
                .map_err(|e| anyhow!(e.to_string()))?;

            let name: String = description.getattr(py, "name")?.extract(py)?;
            let text_description: String = description.getattr(py, "description")?.extract(py)?;
            let python_dependencies: Vec<String> = description
                .getattr(py, "python_dependencies")?
                .extract(py)?;
            let system_dependencies: Vec<String> = description
                .getattr(py, "system_dependencies")?
                .extract(py)?;
            let version: String = description.getattr(py, "version")?.extract(py)?;

            let extractor_schema = ExtractorSchema {
                name,
                version,
                description: text_description,
                python_dependencies,
                system_dependencies,
                embedding_schemas,
                metadata_schemas,
                input_params,
                input_mimes,
            };
            Ok((extractor_wrapper, extractor_schema))
        })?;
        Ok(Self {
            extractor_wrapper,
            extractor_schema,
        })
    }
}

#[async_trait::async_trait]
impl Extractor for PythonExtractor {
    #[tracing::instrument]
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error> {
        Ok(self.extractor_schema.clone())
    }

    fn extract(
        &self,
        content: Vec<internal_api::Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<internal_api::Content>>, anyhow::Error> {
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
            for list1 in py_extracted_data.iter() {
                let mut temp = Vec::new();
                for py_content in list1.iter() {
                    let mime: String = py_content.getattr(py, "content_type")?.extract(py)?;
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
                    let py_labels: Option<PyObject> =
                        py_content.getattr(py, "labels")?.extract(py)?;

                    let labels = match py_labels {
                        Some(py_labels) => {
                            let labels: HashMap<String, String> = py_labels.extract(py)?;
                            labels
                        }
                        None => HashMap::new(),
                    };
                    temp.push(internal_api::Content {
                        mime,
                        bytes: data,
                        feature,
                        labels,
                    });
                }
                extracted_content.push(temp);
            }
            Ok(extracted_content)
        })?;

        Ok(extracted_content)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extract_content() {
        let test_labels = Some(HashMap::from([("url".to_string(), "test.com".to_string())]));
        let content1 = PyContent::new("My name is Donald and I live in Seattle".to_string(), None)
            .try_into()
            .unwrap();
        let content2 = PyContent::new("My name is Donald and I live in Seattle".to_string(), None)
            .try_into()
            .unwrap();
        let content = vec![content1, content2];

        let extractor =
            PythonExtractor::new("indexify_extractor_sdk.mock_extractor", "MockExtractor").unwrap();

        let input_params = serde_json::from_str("{\"a\":5,\"b\":\"recursive\"}").unwrap();

        let extracted_data = extractor.extract(content.clone(), input_params).unwrap();
        assert_eq!(extracted_data.len(), 2);
        assert_eq!(extracted_data.first().unwrap().len(), 3);
        assert_eq!(
            extracted_data.first().unwrap().first().unwrap().mime,
            mime::TEXT_PLAIN.to_string()
        );
        assert_eq!(
            extracted_data.first().unwrap().first().unwrap().labels,
            test_labels.clone().expect("No labels found")
        );

        // Pass in empty input params

        let extracted_data1 = extractor.extract(content, json!({})).unwrap();
        assert_eq!(extracted_data1.len(), 2);
    }

    #[test]
    fn extract_from_blob() {
        let extractor = PythonExtractor::new(
            "indexify_extractor_sdk.mock_extractor",
            "MockExtractorNoInputParams",
        )
        .unwrap();

        let data = std::fs::read("extractors_sdk_tests/data/test.pdf").unwrap();
        let content = PyContent::from_bytes(data, mime::APPLICATION_PDF)
            .try_into()
            .unwrap();
        let extracted_data = extractor.extract(vec![content], json!({})).unwrap();

        assert_eq!(extracted_data.len(), 1);
        //assert_eq!(extracted_data[0].embeddings.len(), 384);
    }

    #[test]
    fn extractor_schema() {
        let extractor =
            PythonExtractor::new("indexify_extractor_sdk.mock_extractor", "MockExtractor").unwrap();

        let extractor_schema = extractor.schemas().unwrap();

        assert_ne!(extractor_schema.input_params, json!({}));

        assert_eq!(
            extractor_schema.input_mimes,
            vec![
                "text/plain".to_string(),
                "application/pdf".to_string(),
                "image/jpeg".to_string()
            ]
        );

        assert_eq!(extractor_schema.version, "0.0.0".to_string());
    }
}
