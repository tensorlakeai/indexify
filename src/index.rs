use std::collections::HashMap;

use anyhow::Result;
use thiserror::Error;

use crate::{EmbeddingGeneratorError, EmbeddingGeneratorTS, VectorDBTS, VectorDbError};

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
    model: String,
}

impl Index {
    pub fn new(
        vectordb: VectorDBTS,
        embedding_generator: EmbeddingGeneratorTS,
        model: String,
    ) -> Result<Index, IndexError> {
        Ok(Self {
            vectordb,
            embedding_generator,
            model,
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
            .generate_embeddings(texts.clone(), String::from(self.model.clone()))
            .await?;
        let it = embeddings.iter().zip(attrs.iter());
        for (i, (x, y)) in it.enumerate() {
            self.vectordb
                .add_embedding(index.clone(), x.to_owned(), y.to_owned())
                .await?;
        }
        Ok(())
    }
}
