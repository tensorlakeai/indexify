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

#[derive(Debug, Clone)]
pub struct Text {
    pub text: String,
    pub metadata: HashMap<String, String>,
}

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

    pub async fn add_texts(&self, index: String, texts: Vec<Text>) -> Result<(), IndexError> {
        let text_content = texts.iter().map(|text| text.text.clone()).collect();
        let embeddings = self
            .embedding_generator
            .generate_embeddings(text_content, self.embedding_model.clone())
            .await?;

        let it = embeddings.iter().zip(texts.iter());
        for (_i, (embedding, text)) in it.enumerate() {
            let mut metadata = text.metadata.to_owned();
            metadata.insert("document".into(), text.text.to_owned());
            self.vectordb
                .add_embedding(index.clone(), embedding.to_owned(), metadata)
                .await?;
        }
        Ok(())
    }

    pub async fn search(
        &self,
        index: String,
        query: String,
        k: u64,
    ) -> Result<Vec<String>, IndexError> {
        let query_embedding = self
            .embedding_generator
            .generate_embeddings(vec![query], self.embedding_model.clone())
            .await?
            .get(0)
            .unwrap()
            .to_owned();

        let results = self.vectordb.search(index, query_embedding, k).await?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{qdrant::QdrantDb, CreateIndexParams, EmbeddingRouter, MetricKind, ServerConfig};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_qdrant_search_basic() {
        let qdrant = QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        });
        qdrant.drop_index("hello".into()).await.unwrap();
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());

        let index_params = CreateIndexParams {
            name: "hello".into(),
            vector_dim: 384,
            metric: MetricKind::Cosine,
        };

        Index::create_index(index_params, qdrant.clone())
            .await
            .unwrap();
        let index = Index::new(qdrant, embedding_router, "all-minilm-l12-v2".into())
            .await
            .unwrap();
        index
            .add_texts(
                "hello".into(),
                vec![
                    Text {
                        text: "hello world".into(),
                        metadata: HashMap::new(),
                    },
                    Text {
                        text: "hello pipe".into(),
                        metadata: HashMap::new(),
                    },
                    Text {
                        text: "nba".into(),
                        metadata: HashMap::new(),
                    },
                ],
            )
            .await
            .unwrap();
        let result = index
            .search("hello".into(), "pipe".into(), 1)
            .await
            .unwrap();
        assert_eq!(1, result.len())
    }
}
