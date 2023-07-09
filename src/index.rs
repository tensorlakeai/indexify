use std::{fmt, str::FromStr, sync::Arc, vec};

use anyhow::Result;
use sea_orm::DatabaseConnection;
use thiserror::Error;
use tracing::debug;
use tracing::error;

use crate::vectordbs::{CreateIndexParams, IndexDistance, VectorChunk, VectorDBTS, VectorDbError};
use crate::{
    entity,
    persistence::{Chunk, Repository, RepositoryError, Text},
    text_splitters::{self, TextSplitterError, TextSplitterKind, TextSplitterTS},
    vectordbs, EmbeddingGeneratorError, EmbeddingGeneratorTS, EmbeddingRouter, VectorIndexConfig,
};

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    EmbeddingGenerator(#[from] EmbeddingGeneratorError),

    #[error(transparent)]
    VectorDb(#[from] VectorDbError),

    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error(transparent)]
    TextSplitter(#[from] text_splitters::TextSplitterError),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),
}

pub struct IndexManager {
    vectordb: VectorDBTS,
    embedding_router: Arc<EmbeddingRouter>,
    repository: Arc<Repository>,
}

impl fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexManager {{ /* fields go here */ }}")
    }
}

pub struct CreateIndexArgs {
    pub name: String,
    pub distance: IndexDistance,
}

impl IndexManager {
    pub fn new(
        repository: Arc<Repository>,
        index_config: VectorIndexConfig,
        embedding_router: Arc<EmbeddingRouter>,
    ) -> Result<Self, IndexError> {
        let vectordb = vectordbs::create_vectordb(index_config)?;
        Ok(IndexManager {
            vectordb,
            embedding_router,
            repository,
        })
    }

    #[allow(dead_code)]
    pub fn new_with_db(
        index_config: VectorIndexConfig,
        embedding_router: Arc<EmbeddingRouter>,
        db: DatabaseConnection,
    ) -> Result<Self, IndexError> {
        let repository = Arc::new(Repository::new_with_db(db));
        IndexManager::new(repository, index_config, embedding_router)
    }

    pub async fn create_index(
        &self,
        index_args: CreateIndexArgs,
        repository_name: &str,
        embedding_model: String,
        text_splitter: TextSplitterKind,
    ) -> Result<(), IndexError> {
        let model = self.embedding_router.get_model(&embedding_model)?;
        // This is to ensure the user is not requesting to create an index
        // with a text splitter that is not supported
        let _ = text_splitters::get_splitter(text_splitter.clone(), model.clone())?;
        let vectordb_params = CreateIndexParams {
            name: index_args.name,
            vector_dim: model.dimensions(),
            distance: index_args.distance,
            unique_params: None,
        };

        self.repository
            .create_index(
                embedding_model,
                vectordb_params,
                self.vectordb.clone(),
                text_splitter.to_string(),
                repository_name,
            )
            .await?;
        Ok(())
    }

    pub async fn load(&self, index_name: &str) -> Result<Index, IndexError> {
        let index_entity = self.repository.get_index(index_name).await?;
        let model = self
            .embedding_router
            .get_model(&index_entity.embedding_model)?;
        let text_splitter_kind = TextSplitterKind::from_str(&index_entity.text_splitter)
            .map_err(|e| TextSplitterError::UnknownSplitterKind(e.to_string()))?;
        let text_splitter = text_splitters::get_splitter(text_splitter_kind, model.clone())?;

        Index::new(
            index_name.into(),
            self.vectordb.clone(),
            self.repository.clone(),
            text_splitter,
            model.clone(),
        )
        .await
    }
}
pub struct Index {
    name: String,
    vectordb: VectorDBTS,
    repository: Arc<Repository>,
    text_splitter: TextSplitterTS,
    embedding_generator: EmbeddingGeneratorTS,
}

impl Index {
    pub async fn new(
        name: String,
        vectordb: VectorDBTS,
        repository: Arc<Repository>,
        text_splitter: TextSplitterTS,
        embedding_generator: EmbeddingGeneratorTS,
    ) -> Result<Index, IndexError> {
        Ok(Self {
            name,
            vectordb,
            repository,
            text_splitter,
            embedding_generator,
        })
    }

    pub async fn add_to_index(&self, content: entity::content::Model) -> Result<(), IndexError> {
        let text = content.text;
        let splitted_texts = self.text_splitter.split(&text, 1000, 0).await?;
        let embeddings = self
            .embedding_generator
            .generate_embeddings(splitted_texts.clone())
            .await?;
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut vector_chunks: Vec<VectorChunk> = Vec::new();
        for (text, embedding) in splitted_texts.iter().zip(embeddings.iter()) {
            debug!("adding to index {}, text: {}", self.name, text);
            let chunk = Chunk::new(text.to_string(), content.id.clone());
            let vector_chunk =
                VectorChunk::new(chunk.chunk_id.clone(), text.to_string(), embedding.to_vec());
            chunks.push(chunk);
            vector_chunks.push(vector_chunk);
        }
        self.vectordb
            .add_embedding(&self.name, vector_chunks)
            .await?;
        self.repository
            .create_chunks(&content.id, chunks, &self.name)
            .await?;

        Ok(())
    }

    pub async fn search(&self, query: &str, k: u64) -> Result<Vec<Text>, IndexError> {
        let query_embedding = self
            .embedding_generator
            .generate_embeddings(vec![query.into()])
            .await?
            .get(0)
            .unwrap()
            .to_owned();
        let results = self
            .vectordb
            .search(self.name.clone(), query_embedding, k)
            .await?;
        let mut index_search_results = Vec::new();
        for result in results {
            let chunk = self.repository.chunk_with_id(&result.chunk_id).await;
            if chunk.as_ref().is_err() {
                error!("Chunk with id {} not found", result.chunk_id);
                continue;
            }
            let search_result = Text {
                id: chunk.as_ref().unwrap().content_id.clone(),
                text: chunk.as_ref().unwrap().text.clone(),
                metadata: chunk.as_ref().unwrap().metadata.clone(),
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::env;

    use crate::data_repository_manager::DataRepositoryManager;
    use crate::persistence::{DataRepository, Text};
    use crate::test_util;
    use crate::test_util::db_utils::create_index_manager;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_qdrant_search_basic() {
        env::set_var("RUST_LOG", "debug");
        let index_name = "default/default";
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, embedding_runner) = create_index_manager(db.clone(), index_name).await;
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), index_manager.clone());
        repository_manager
            .sync(&DataRepository::default())
            .await
            .unwrap();
        let repository = "default";
        repository_manager
            .add_texts(
                repository,
                vec![
                    Text::from_text(repository, "hello world", None, HashMap::new()),
                    Text::from_text(repository, "hello pipe", None, HashMap::new()),
                    Text::from_text(repository, "nba", None, HashMap::new()),
                ],
                None,
            )
            .await
            .unwrap();
        repository_manager
            .add_texts(
                repository,
                vec![Text::from_text(
                    repository,
                    "hello world",
                    None,
                    HashMap::new(),
                )],
                None,
            )
            .await
            .unwrap();

        embedding_runner.sync_repo("default").await.unwrap();
        let index = index_manager.load("default/default").await.unwrap();
        let result = index.search("pipe", 1).await.unwrap();
        assert_eq!(1, result.len())
    }
}
