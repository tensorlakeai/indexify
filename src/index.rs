use std::collections::HashMap;

use anyhow::Result;
use sea_orm::DatabaseConnection;
use thiserror::Error;
use tracing::info;

use crate::{
    persistence::{Respository, RespositoryError},
    text_splitters::{self, TextSplitterTS},
    vectordbs, CreateIndexParams, EmbeddingGeneratorError, EmbeddingGeneratorTS, SearchResult,
    VectorDBTS, VectorDbError, VectorIndexConfig,
};

#[async_trait::async_trait]
pub trait Indexstore {
    async fn get_index(name: String) -> Result<Index, IndexError>;

    async fn store_index(name: String, splitter: String) -> Result<(), IndexError>;
}

#[derive(Debug, Clone)]
pub struct Text {
    pub texts: Vec<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    EmbeddingGenerator(#[from] EmbeddingGeneratorError),

    #[error(transparent)]
    VectorDb(#[from] VectorDbError),

    #[error(transparent)]
    Persistence(#[from] RespositoryError),

    #[error(transparent)]
    TextSplitter(#[from] text_splitters::TokenizerError),
}

pub struct IndexManager {
    vectordb: VectorDBTS,
    embedding_router: EmbeddingGeneratorTS,
    repository: Respository,
}

impl IndexManager {
    pub async fn new(
        index_config: Option<VectorIndexConfig>,
        embedding_router: EmbeddingGeneratorTS,
    ) -> Result<Option<Self>, IndexError> {
        if index_config.is_none() {
            info!("indexing feature is not configured");
            return Ok(None);
        }
        let db_url = &index_config.clone().unwrap().db_url;
        info!("persistence: using database: {:?}", db_url);
        let repository = Respository::new(db_url).await?;
        info!(
            "vector database backend: {:?}",
            index_config.as_ref().unwrap().index_store
        );
        IndexManager::_new(repository, index_config.unwrap(), embedding_router)
    }

    fn _new(
        repository: Respository,
        index_config: VectorIndexConfig,
        embedding_router: EmbeddingGeneratorTS,
    ) -> Result<Option<Self>, IndexError> {
        let vectordb = vectordbs::create_vectordb(index_config)?;
        Ok(Some(IndexManager {
            vectordb,
            embedding_router,
            repository,
        }))
    }

    #[allow(dead_code)]
    pub fn new_with_db(
        index_config: Option<VectorIndexConfig>,
        embedding_router: EmbeddingGeneratorTS,
        db: DatabaseConnection,
    ) -> Result<Option<Self>, IndexError> {
        let repository = Respository::new_with_db(db);
        IndexManager::_new(repository, index_config.unwrap(), embedding_router)
    }

    pub async fn create_index(
        &self,
        vectordb_params: CreateIndexParams,
        embedding_model: String,
        text_splitter: String,
    ) -> Result<(), IndexError> {
        self.repository
            .create_index(
                embedding_model,
                vectordb_params,
                self.vectordb.clone(),
                text_splitter,
            )
            .await?;
        Ok(())
    }

    pub async fn load(&self, index_name: String) -> Result<Option<Index>, IndexError> {
        let index_entity = self.repository.get_index(index_name.clone()).await?;
        let splitter = text_splitters::get_splitter(&index_entity.text_splitter)?;
        let index = Index::new(
            index_name.clone(),
            self.vectordb.clone(),
            self.embedding_router.clone(),
            index_entity.embedding_model,
            splitter,
        )
        .await?;
        Ok(index)
    }
}
pub struct Index {
    name: String,
    vectordb: VectorDBTS,
    embedding_generator: EmbeddingGeneratorTS,
    embedding_model: String,
    text_splitter: TextSplitterTS,
}

impl Index {
    pub async fn new(
        name: String,
        vectordb: VectorDBTS,
        embedding_generator: EmbeddingGeneratorTS,
        embedding_model: String,
        text_splitter: TextSplitterTS,
    ) -> Result<Option<Index>, IndexError> {
        Ok(Some(Self {
            name,
            vectordb,
            embedding_generator,
            embedding_model,
            text_splitter,
        }))
    }

    pub async fn add_texts(&self, texts: Vec<Text>) -> Result<(), IndexError> {
        for mut text in texts {
            let mut splitted_texts = Vec::new();
            for doc in text.texts {
                let s_text = self.text_splitter.split(&doc, 1000, 1000)?;
                splitted_texts.extend(s_text);
            }
            text.texts = splitted_texts;

            let embeddings = self
                .embedding_generator
                .generate_embeddings(text.texts.clone(), self.embedding_model.clone())
                .await?;
            self.vectordb
                .add_embedding(&self.name, embeddings, text.texts, text.metadata)
                .await?;
        }
        Ok(())
    }

    pub async fn search(&self, query: String, k: u64) -> Result<Vec<SearchResult>, IndexError> {
        let query_embedding = self
            .embedding_generator
            .generate_embeddings(vec![query], self.embedding_model.clone())
            .await?
            .get(0)
            .unwrap()
            .to_owned();

        let results = self
            .vectordb
            .search(self.name.clone(), query_embedding, k)
            .await?;
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::super::entity::index::Entity as IndexEntity;
    use sea_orm::entity::prelude::*;
    use sea_orm::{
        sea_query::TableCreateStatement, Database, DatabaseConnection, DbBackend, DbConn, DbErr,
        Schema,
    };

    use super::*;
    use std::sync::Arc;

    use crate::{
        qdrant::QdrantDb, CreateIndexParams, EmbeddingRouter, MetricKind, QdrantConfig,
        ServerConfig, VectorIndexConfig,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_qdrant_search_basic() {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        qdrant.drop_index("hello".into()).await.unwrap();
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());

        let index_params = CreateIndexParams {
            name: "hello".into(),
            vector_dim: 384,
            metric: MetricKind::Cosine,
        };
        let index_config = Some(VectorIndexConfig {
            index_store: crate::IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig {
                addr: "http://localhost:6334".into(),
            }),
            db_url: "sqlite::memory:".into(),
        });
        let db = create_db().await.unwrap();
        let index_manager = IndexManager::new_with_db(index_config, embedding_router, db)
            .unwrap()
            .unwrap();
        index_manager
            .create_index(index_params, "all-minilm-l12-v2".into(), "noop".into())
            .await
            .unwrap();
        let index = index_manager.load("hello".into()).await.unwrap().unwrap();
        index
            .add_texts(vec![
                Text {
                    texts: vec!["hello world".into()],
                    metadata: HashMap::new(),
                },
                Text {
                    texts: vec!["hello pipe".into()],
                    metadata: HashMap::new(),
                },
                Text {
                    texts: vec!["nba".into()],
                    metadata: HashMap::new(),
                },
            ])
            .await
            .unwrap();
        let result = index.search("pipe".into(), 1).await.unwrap();
        assert_eq!(1, result.len())
    }

    async fn create_db() -> Result<DatabaseConnection, DbErr> {
        let db = Database::connect("sqlite::memory:").await?;

        setup_schema(&db).await?;

        Ok(db)
    }

    async fn setup_schema(db: &DbConn) -> Result<(), DbErr> {
        // Setup Schema helper
        let schema = Schema::new(DbBackend::Sqlite);

        // Derive from Entity
        let stmt1: TableCreateStatement = schema.create_table_from_entity(IndexEntity);

        // Execute create table statement
        db.execute(db.get_database_backend().build(&stmt1)).await?;
        Ok(())
    }
}
