use std::{collections::HashMap};

use anyhow::Result;
use thiserror::Error;

use crate::{
    CreateIndexParams, EmbeddingGeneratorError, EmbeddingGeneratorTS, VectorDBTS, VectorDbError,
};

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    EmbeddingError(#[from] EmbeddingGeneratorError),

    #[error(transparent)]
    VectorDbError(#[from] VectorDbError),
}
pub struct Index {
    vectordb: VectorDBTS,
    embedding_generator: EmbeddingGeneratorTS,
    embedding_model: String,
}

impl Index {
    pub async fn new(
        vectordb_params: CreateIndexParams,
        vectordb: VectorDBTS,
        embedding_generator: EmbeddingGeneratorTS,
        embedding_model: String,
    ) -> Result<Index, IndexError> {
        vectordb.create_index(vectordb_params).await?;
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
}
