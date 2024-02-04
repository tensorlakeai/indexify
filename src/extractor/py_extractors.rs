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
    pub features: Vec<PyFeature>,
    #[pyo3(get, set)]
    pub labels: HashMap<String, String>,
}

impl TryFrom<PyContent> for internal_api::Content {
    type Error = anyhow::Error;

    fn try_from(py_content: PyContent) -> Result<Self, Self::Error> {
        let mime_type = mime::Mime::from_str(&py_content.content_type)?;
        let mut features = Vec::new();
        for feature in py_content.features.iter() {
            let feature_type = match feature.feature_type.as_str() {
                "embedding" => internal_api::FeatureType::Embedding,
                "metadata" => internal_api::FeatureType::Metadata,
                _ => internal_api::FeatureType::Unknown,
            };
            let data = serde_json::from_str(&feature.value)?;
            let name = feature.name.clone();
            let feature = internal_api::Feature {
                feature_type,
                name,
                data,
            };
            features.push(feature);
        }
        let extracted_content = internal_api::Content {
            mime: mime_type.to_string(),
            bytes: py_content.data,
            features,
            labels: py_content.labels,
        };
        Ok(extracted_content)
    }
}

impl TryFrom<internal_api::Content> for PyContent {
    type Error = anyhow::Error;

    fn try_from(content: internal_api::Content) -> Result<Self, Self::Error> {
        let content_type = content.mime.to_string();
        let data = content.bytes;
        let mut py_features = Vec::new();
        for feature in content.features {
            let feature_type = match feature.feature_type {
                internal_api::FeatureType::Embedding => "embedding",
                internal_api::FeatureType::Metadata => "metadata",
                _ => "unknown",
            };
            let name = feature.name;
            let value = serde_json::to_string(&feature.data)?;
            let py_feature = PyFeature {
                feature_type: feature_type.to_string(),
                name,
                value,
            };
            py_features.push(py_feature);
        }
        Ok(PyContent {
            content_type,
            data,
            features: py_features,
            labels: content.labels,
        })
    }
}

impl PyContent {
    pub fn new(data: String, labels: HashMap<String, String>) -> Self {
        Self {
            content_type: mime::TEXT_PLAIN.to_string(),
            data: data.into_bytes().to_vec(),
            features: vec![],
            labels,
        }
    }

    pub fn from_bytes(data: Vec<u8>, content_type: mime::Mime) -> Self {
        Self {
            content_type: content_type.to_string(),
            data,
            features: vec![],
            labels: HashMap::new(),
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
            );
            if let Err(err) = &module {
                let msg = err.traceback(py).map(|t| t.format().unwrap()).unwrap();
                return Err(anyhow!(msg));
            }
            let module = module?;
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
                    let py_features: Vec<PyObject> =
                        py_content.getattr(py, "features")?.extract(py)?;
                    let mut features = Vec::new();
                    for py_feature in py_features {
                        let feature_type: String =
                            py_feature.getattr(py, "feature_type")?.extract(py)?;
                        let feature_type = internal_api::FeatureType::from_str(&feature_type)?;
                        let name: String = py_feature.getattr(py, "name")?.extract(py)?;
                        let value: String = py_feature.getattr(py, "value")?.extract(py)?;
                        let value: serde_json::Value = serde_json::from_str(&value)?;
                        features.push(internal_api::Feature {
                            feature_type,
                            name,
                            data: value,
                        });
                    }
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
                        features,
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
    #[ignore]
    fn extract_content() {
        let test_labels = Some(HashMap::from([("url".to_string(), "test.com".to_string())]));
        let content1 = PyContent::new(
            "My name is Donald and I live in Seattle".to_string(),
            HashMap::new(),
        )
        .try_into()
        .unwrap();
        let content2 = PyContent::new(
            "My name is Donald and I live in Seattle".to_string(),
            HashMap::new(),
        )
        .try_into()
        .unwrap();
        let content = vec![content1, content2];

        let extractor =
            PythonExtractor::new("indexify_extractor_sdk.mock_extractor", "MockExtractor").unwrap();

        let input_params = serde_json::from_str("{\"a\":5,\"b\":\"recursive\"}").unwrap();

        let extracted_data = extractor.extract(content.clone(), input_params).unwrap();
        assert_eq!(extracted_data.len(), 2);
        assert_eq!(extracted_data.first().unwrap().len(), 2);
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
    #[ignore]
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
    #[ignore]
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
