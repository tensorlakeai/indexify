use anyhow::{anyhow, Result};
use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyList},
};

use crate::{EmbeddingGenerator, EmbeddingGeneratorError};

pub struct SentenceTransformerModel {
    dpr_object: PyObject,
}

impl SentenceTransformerModel {
    pub fn new(model_name: String) -> Result<Self> {
        Python::with_gil(|py| {
            let syspath: &PyList = py
                .import("sys")?
                .getattr("path")?
                .downcast()
                .map_err(|e| anyhow!(e.to_string()))?;
            syspath.insert(0, ".")?;
            let module = PyModule::import(py, "indexify_py.sentence_transformer")?;
            let dpr_class = module.getattr("SentenceTransformersEmbedding")?;
            let kwargs = [("model_name", model_name)].into_py_dict(py);
            let dpr_object = dpr_class.call((), Some(kwargs))?.into_py(py);
            Ok(SentenceTransformerModel { dpr_object })
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

    pub fn embed_ctx(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>> {
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

    pub fn encode_tokens(&self, inputs: Vec<String>) -> Result<Vec<Vec<u64>>> {
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

    pub fn decode_tokens(&self, tokens: Vec<Vec<u64>>) -> Result<Vec<String>> {
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

pub struct AllMiniLmL12V2 {
    st: SentenceTransformerModel,
}

impl AllMiniLmL12V2 {
    pub fn new() -> Result<Self, EmbeddingGeneratorError> {
        let model_name: String = "all-MiniLM-L12-v2".into();
        let st = SentenceTransformerModel::new(model_name)
            .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
        Ok(Self { st })
    }
}

#[async_trait::async_trait]
impl EmbeddingGenerator for AllMiniLmL12V2 {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        self.st
            .embed_ctx(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    fn dimensions(&self) -> u64 {
        384_u64
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self.st
            .encode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.st
            .decode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }
}

pub struct AllMiniLmL6V2 {
    st: SentenceTransformerModel,
}

impl AllMiniLmL6V2 {
    pub fn new() -> Result<Self, EmbeddingGeneratorError> {
        let model_name: String = "all-MiniLM-L6-v2".into();
        let st = SentenceTransformerModel::new(model_name)
            .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
        Ok(Self { st })
    }
}

#[async_trait::async_trait]
impl EmbeddingGenerator for AllMiniLmL6V2 {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        self.st
            .embed_ctx(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    fn dimensions(&self) -> u64 {
        384_u64
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self.st
            .encode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.st
            .decode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }
}

pub struct T5Base {
    st: SentenceTransformerModel,
}

impl T5Base {
    pub fn new() -> Result<Self, EmbeddingGeneratorError> {
        let model_name: String = "sentence-t5-base".into();
        let st = SentenceTransformerModel::new(model_name)
            .map_err(|e| EmbeddingGeneratorError::ModelLoadingError(e.to_string()))?;
        Ok(Self { st })
    }
}

#[async_trait::async_trait]
impl EmbeddingGenerator for T5Base {
    async fn generate_embeddings(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, EmbeddingGeneratorError> {
        self.st
            .embed_ctx(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    fn dimensions(&self) -> u64 {
        384_u64
    }

    async fn tokenize_encode(
        &self,
        inputs: Vec<String>,
    ) -> Result<Vec<Vec<u64>>, EmbeddingGeneratorError> {
        self.st
            .encode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }

    async fn tokenize_decode(
        &self,
        inputs: Vec<Vec<u64>>,
    ) -> Result<Vec<String>, EmbeddingGeneratorError> {
        self.st
            .decode_tokens(inputs)
            .map_err(|e| EmbeddingGeneratorError::ModelError(e.to_string()))
    }
}
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[tokio::test]
    async fn test_generate_embeddings_all_mini_lm_l12v2() {
        let inputs = vec![
            "Hello, world!".into(),
            "Hello, NBA!".into(),
            "Hello, NFL!".into(),
        ];
        let embedding_generator = AllMiniLmL12V2::new().unwrap();
        let embeddings = embedding_generator
            .generate_embeddings(inputs)
            .await
            .unwrap();
        assert_eq!(embeddings.len(), 3);
        assert_eq!(embeddings[0].len(), 384);
    }

    #[tokio::test]
    async fn test_tokenization_encode_decode() {
        let embedding_generator = AllMiniLmL12V2::new().unwrap();
        let inputs = vec!["embiid is the mvp".into()];
        let tokens = embedding_generator
            .tokenize_encode(inputs.clone())
            .await
            .unwrap();

        let tokenized_text = embedding_generator.tokenize_decode(tokens).await.unwrap();
        assert_eq!(inputs, tokenized_text);
    }
}
