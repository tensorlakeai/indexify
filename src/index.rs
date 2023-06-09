use std::{collections::HashMap, fmt, str::FromStr, sync::Arc, vec};

use anyhow::Result;
use sea_orm::DatabaseConnection;
use thiserror::Error;
use tracing::info;
use uuid::Uuid;

use crate::{
    embedding_worker::EmbeddingWorker,
    persistence::{Repository, RepositoryError, Text},
    text_splitters::{self, TextSplitterKind},
    vectordbs, CreateIndexParams, EmbeddingGeneratorError, EmbeddingGeneratorTS, EmbeddingRouter,
    SearchResult, VectorDBTS, VectorDbError, VectorIndexConfig,
};

#[async_trait::async_trait]
pub trait Indexstore {
    async fn get_index(name: String) -> Result<Index, IndexError>;

    async fn store_index(name: String, splitter: String) -> Result<(), IndexError>;
}

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

    #[error("logic error: `{0}`")]
    LogicError(String),
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

impl IndexManager {
    pub async fn new(
        index_config: VectorIndexConfig,
        embedding_router: Arc<EmbeddingRouter>,
        db_url: String,
    ) -> Result<Self, IndexError> {
        info!("persistence: using database: {}", &db_url);
        let repository = Arc::new(Repository::new(&db_url).await?);
        info!("vector database backend: {}", index_config.index_store);
        IndexManager::_new(repository, index_config, embedding_router)
    }

    fn _new(
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
        IndexManager::_new(repository, index_config, embedding_router)
    }

    pub async fn create_index(
        &self,
        vectordb_params: CreateIndexParams,
        embedding_model: String,
        text_splitter: TextSplitterKind,
    ) -> Result<(), IndexError> {
        let model = self.embedding_router.get_model(embedding_model.clone())?;
        // This is to ensure the user is not requesting to create an index
        // with a text splitter that is not supported
        let _ = text_splitters::get_splitter(text_splitter.clone(), model)?;

        self.repository
            .create_index(
                embedding_model,
                vectordb_params,
                self.vectordb.clone(),
                text_splitter.to_string(),
            )
            .await?;
        Ok(())
    }

    pub async fn load(&self, index_name: String) -> Result<Option<Index>, IndexError> {
        let index_entity = self.repository.get_index(index_name.clone()).await?;
        let splitter_kind = TextSplitterKind::from_str(&index_entity.text_splitter)
            .map_err(|e| IndexError::LogicError(e.to_string()))?;
        let model = self
            .embedding_router
            .get_model(index_entity.embedding_model)?;
        let splitter = text_splitters::get_splitter(splitter_kind, model.clone())?;
        let embedding_worker = Arc::new(EmbeddingWorker::new(
            self.repository.clone(),
            self.vectordb.clone(),
            model.clone(),
            splitter.clone(),
        ));
        let index = Index::new(
            index_name.clone(),
            embedding_worker,
            self.vectordb.clone(),
            self.repository.clone(),
            model.clone(),
        )
        .await?;
        Ok(index)
    }

    pub async fn create_index_for_memory_session(
        &self,
        session_id: Uuid,
        index_name: String,
        metadata: HashMap<String, String>,
        vectordb_params: CreateIndexParams,
        embedding_model: String,
        text_splitter: TextSplitterKind,
    ) -> Result<(), IndexError> {
        let vectordb = self.vectordb.clone();
        self.repository
            .create_memory_session(
                session_id,
                index_name,
                metadata,
                vectordb_params,
                embedding_model,
                vectordb,
                text_splitter.to_string(),
            )
            .await
            .map_err(|e| IndexError::Persistence(e))
    }

    pub async fn get_index_name_for_memory_session(
        &self,
        session_id: Uuid,
    ) -> Result<String, IndexError> {
        self.repository
            .get_index_name_for_memory_session(session_id)
            .await
            .map_err(|e| IndexError::Persistence(e))
    }
}
pub struct Index {
    name: String,
    embedding_worker: Arc<EmbeddingWorker>,
    vectordb: VectorDBTS,
    repository: Arc<Repository>,
    embedding_generator: EmbeddingGeneratorTS,
}

impl Index {
    pub async fn new(
        name: String,
        embedding_worker: Arc<EmbeddingWorker>,
        vectordb: VectorDBTS,
        repository: Arc<Repository>,
        embedding_generator: EmbeddingGeneratorTS,
    ) -> Result<Option<Index>, IndexError> {
        Ok(Some(Self {
            name,
            embedding_worker,
            vectordb,
            repository,
            embedding_generator,
        }))
    }

    pub async fn add_texts(&self, texts: Vec<Text>) -> Result<(), IndexError> {
        self.repository
            .add_to_index(self.name.clone(), texts)
            .await?;
        self.embedding_worker
            .run_once()
            .await
            .map_err(|e| IndexError::LogicError(e.to_string()))?;
        Ok(())
    }

    pub async fn get_texts(&self) -> Result<Vec<Text>, IndexError> {
        let texts = self.repository.get_texts(self.name.clone()).await?;
        Ok(texts)
    }

    pub async fn search(&self, query: String, k: u64) -> Result<Vec<SearchResult>, IndexError> {
        let query_embedding = self
            .embedding_generator
            .generate_embeddings(vec![query])
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
    use super::super::entity::content::Entity as ContentEntity;
    use super::super::entity::index::Entity as IndexEntity;
    use super::super::entity::index_chunks::Entity as IndexChunkEntity;
    use sea_orm::entity::prelude::*;
    use sea_orm::{
        sea_query::TableCreateStatement, Database, DatabaseConnection, DbBackend, DbConn, DbErr,
        Schema,
    };

    use super::*;
    use std::collections::HashMap;
    use std::env;
    use std::sync::Arc;

    use crate::{
        qdrant::QdrantDb, CreateIndexParams, EmbeddingRouter, MetricKind, QdrantConfig,
        ServerConfig, VectorIndexConfig,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_qdrant_search_basic() {
        env::set_var("RUST_LOG", "debug");
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
            unique_params: None,
        };
        let index_config = VectorIndexConfig {
            index_store: crate::IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig {
                addr: "http://localhost:6334".into(),
            }),
        };
        let db = create_db().await.unwrap();
        let index_manager = IndexManager::new_with_db(index_config, embedding_router, db).unwrap();
        index_manager
            .create_index(
                index_params,
                "all-minilm-l12-v2".into(),
                TextSplitterKind::Noop,
            )
            .await
            .unwrap();
        let index = index_manager.load("hello".into()).await.unwrap().unwrap();
        index
            .add_texts(vec![
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
            ])
            .await
            .unwrap();
        index
            .add_texts(vec![Text {
                text: "hello world".into(),
                metadata: HashMap::new(),
            }])
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
        let stmt2: TableCreateStatement = schema.create_table_from_entity(ContentEntity);
        let stmt3: TableCreateStatement = schema.create_table_from_entity(IndexChunkEntity);

        // Execute create table statement
        db.execute(db.get_database_backend().build(&stmt1)).await?;
        db.execute(db.get_database_backend().build(&stmt2)).await?;
        db.execute(db.get_database_backend().build(&stmt3)).await?;
        Ok(())
    }
}
