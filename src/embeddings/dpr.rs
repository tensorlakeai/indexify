use anyhow::{anyhow, Ok, Result};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList},
};

use crate::{EmbeddingGenerator, EmbeddingGeneratorError};

pub struct DPREmbeddings {
    dpr_object: PyObject,
}

impl DPREmbeddings {
    pub fn new() -> Result<Self> {
        Python::with_gil(|py| {
            let syspath: &PyList = py
                .import("sys")?
                .getattr("path")?
                .downcast()
                .map_err(|e| anyhow!(e.to_string()))?;
            syspath.insert(0, ".")?;
            let module = PyModule::import(py, "indexify_py.dpr")?;
            let dpr_class = module.getattr("DPREmbeddings")?;
            let dpr_object = dpr_class.call0()?.into_py(py);
            Ok(DPREmbeddings { dpr_object })
        })
    }

    pub fn embed_query(&self, query: String) -> Result<Vec<f32>> {
        Python::with_gil(|py| {
            let kwargs = [("input", query)].into_py_dict(py);
            let result: Vec<f32> = self
                .dpr_object
                .getattr(py, "embed_query")?
                .call(py, (), Some(kwargs))?
                .extract(py)?;
            Ok(result)
        })
    }

    pub fn _embed_ctx(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>> {
        Python::with_gil(|py| {
            let kwargs = [("inputs", inputs)].into_py_dict(py);
            let result: Vec<Vec<f32>> = self
                .dpr_object
                .getattr(py, "embed_ctx")?
                .call(py, (), Some(kwargs))?
                .extract(py)?;
            Ok(result)
        })
    }

    pub fn dimensions(&self) -> u64 {
        768
    }

    pub fn _encode_tokens(&self, inputs: Vec<String>) -> Result<Vec<Vec<u64>>> {
        Python::with_gil(|py| {
            let kwargs = [("inputs", inputs)].into_py_dict(py);
            let result: Vec<Vec<u64>> = self
                .dpr_object
                .getattr(py, "tokenizer_encode")?
                .call(py, (), Some(kwargs))?
                .extract(py)?;
            Ok(result)
        })
    }

    pub fn _decode_tokens(&self, tokens: Vec<Vec<u64>>) -> Result<Vec<String>> {
        Python::with_gil(|py| {
            let kwargs = [("tokens", tokens)].into_py_dict(py);
            let result: Vec<String> = self
                .dpr_object
                .getattr(py, "tokenizer_decode")?
                .call(py, (), Some(kwargs))?
                .extract(py)?;
            Ok(result)
        })
    }
}

#[async_trait::async_trait]
impl EmbeddingGenerator for DPREmbeddings {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        self._embed_ctx(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    fn dimensions(&self) -> u64 {
        786
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self._encode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self._decode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_dpr() {
        let dpr = DPREmbeddings::new().unwrap();

        let query = "What is the capital of France?";
        let ctx = vec!["Paris is the capital of France.".into()];

        let query_embedding = dpr.embed_query(query.into()).unwrap();
        assert_eq!(query_embedding.len(), 768);

        let ctx_embedding = dpr.generate_embeddings(ctx).await.unwrap();
        assert_eq!(ctx_embedding.len(), 1);
        assert_eq!(ctx_embedding[0].len(), 768);
    }
}
