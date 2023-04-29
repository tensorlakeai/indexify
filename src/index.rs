use std::collections::HashMap;

use anyhow::Result;
use thiserror::Error;

use crate::{
    CreateIndexParams, EmbeddingGeneratorError, EmbeddingGeneratorTS, VectorDBTS, VectorDbError,
};

#[async_trait::async_trait]
pub trait Indexstore {
    async fn get_index(name: String) -> Result<Index, IndexError>;

    async fn store_index(name: String, splitter: String) -> Result<(), IndexError>;
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    EmbeddingError(#[from] EmbeddingGeneratorError),

    #[error(transparent)]
    VectorDbError(#[from] VectorDbError),

    #[error("index not found: `{0}`")]
    IndexNotFound(String),
}
pub struct Index {
    vectordb: VectorDBTS,
    embedding_generator: EmbeddingGeneratorTS,
    embedding_model: String,
}

impl Index {
    pub async fn create_index(
        vectordb_params: CreateIndexParams,
        vectordb: VectorDBTS,
    ) -> Result<(), IndexError> {
        vectordb.create_index(vectordb_params).await?;
        Ok(())
    }

    pub async fn new(
        vectordb: VectorDBTS,
        embedding_generator: EmbeddingGeneratorTS,
        embedding_model: String,
    ) -> Result<Index, IndexError> {
        Ok(Self {
            vectordb,
            embedding_generator,
            embedding_model,
        })
    }
    pub async fn add_texts(
        &self,
        index: String,
        texts: Vec<String>,
        attrs: Vec<HashMap<String, String>>,
    ) -> Result<(), IndexError> {
        let embeddings = self
            .embedding_generator
            .generate_embeddings(texts.clone(), self.embedding_model.clone())
            .await?;
        let it = embeddings.iter().zip(attrs.iter());
        for (_i, (embedding, attr)) in it.enumerate() {
            self.vectordb
                .add_embedding(index.clone(), embedding.to_owned(), attr.to_owned())
                .await?;
        }
        Ok(())
    }

    pub async fn search(
        &self,
        embedding_model: String,
        query: String,
        index: String,
        k: u64,
    ) -> Result<Vec<String>, IndexError> {
        let query_embedding = self
            .embedding_generator
            .generate_embeddings(vec![query], embedding_model)
            .await?
            .get(0)
            .unwrap()
            .to_owned();
        let results = self.vectordb.search(index, query_embedding, k).await?;
        Ok(results)
    }
}
