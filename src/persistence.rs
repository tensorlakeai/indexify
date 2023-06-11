use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use entity::data_repository::Entity as DataRepositoryEntity;
use entity::index::Entity as IndexEntity;
use entity::index::Model as IndexModel;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveModelTrait, ColumnTrait};
use sea_orm::{
    ActiveValue::NotSet, Database, DatabaseConnection, DbErr, EntityTrait, Set, TransactionTrait,
};
use sea_orm::{DatabaseTransaction, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::json;
use strum_macros::{Display, EnumString};
use thiserror::Error;

use crate::entity::index;
use crate::text_splitters::TextSplitterKind;
use crate::vectordbs::{self, CreateIndexParams};
use crate::{entity, IndexDistance};

#[derive(Serialize, Deserialize, Default)]
struct ExtractorsState {
    #[serde(default)]
    state: HashMap<String, u64>,
}

impl ExtractorsState {
    fn update(&mut self, extractor_name: &str) {
        let current_state = self.state.entry(extractor_name.to_string()).or_insert(0);
        *current_state += 1;
    }
}

#[derive(Clone, Error, Debug, Display, EnumString, Serialize, Deserialize)]
pub enum ContentType {
    #[strum(serialize = "text")]
    Text,

    #[strum(serialize = "memory")]
    Memory,
}

#[derive(Debug, Clone)]
pub struct Text {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "extractor_type")]
pub enum ExtractorType {
    #[serde(rename = "embedding")]
    Embedding {
        model: String,
        text_splitter: TextSplitterKind,
        distance: IndexDistance,
    },

    #[serde(rename = "ner")]
    Ner { model: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "extractor")]
pub struct ExtractorConfig {
    pub name: String,
    pub extractor_type: ExtractorType,
    pub content_type: ContentType,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            content_type: ContentType::Text,
            extractor_type: ExtractorType::Embedding {
                model: "all-minilm-l12-v2".to_string(),
                text_splitter: TextSplitterKind::Noop,
                distance: IndexDistance::Cosine,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "source_type")]
pub enum SourceType {
    // todo: replace metadata with actual request parameters for GoogleContactApi
    #[serde(rename = "google_contact")]
    GoogleContact { metadata: Option<String> },
    // todo: replace metadata with actual request parameters for gmail API
    #[serde(rename = "gmail")]
    Gmail { metadata: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "data_connector")]
pub struct DataConnector {
    pub source: SourceType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRepository {
    pub name: String,
    pub data_connectors: Vec<DataConnector>,
    pub extractors: Vec<ExtractorConfig>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Default for DataRepository {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            data_connectors: vec![],
            extractors: vec![ExtractorConfig::default()],
            metadata: HashMap::new(),
        }
    }
}

impl From<entity::data_repository::Model> for DataRepository {
    fn from(model: entity::data_repository::Model) -> Self {
        let extractors = model
            .extractors
            .map(|s| serde_json::from_str(&s).unwrap())
            .unwrap_or_default();
        let data_connectors = model
            .data_connectors
            .map(|s| serde_json::from_str(&s).unwrap())
            .unwrap_or_default();
        let metadata = model
            .metadata
            .map(|s| serde_json::from_str(&s).unwrap())
            .unwrap_or_default();
        Self {
            name: model.name,
            extractors,
            data_connectors,
            metadata,
        }
    }
}

pub struct ChunkWithMetadata {
    pub chunk_id: String,
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub text: String,
    pub chunk_id: String,
    pub content_id: String,
}

impl Chunk {
    pub fn new(text: String, content_id: String) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        text.hash(&mut s);
        let chunk_id = format!("{:x}", s.finish());
        Self {
            text,
            chunk_id,
            content_id,
        }
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error(transparent)]
    DatabaseError(#[from] DbErr),

    #[error(transparent)]
    VectorDb(#[from] vectordbs::VectorDbError),

    #[error("repository `{0}` not found")]
    RepositoryNotFound(String),

    #[error("index `{0}` not found")]
    IndexNotFound(String),

    #[error("content`{0}` not found")]
    ContentNotFound(String),

    #[error("chunk `{0}` not found")]
    ChunkNotFound(String),

    #[error("index `{0}` already exists")]
    IndexAlreadyExists(String),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),

    #[error("session `{0}` not found")]
    SessionNotFound(String),

    #[error("internal application error `{0}`")]
    LogicError(String),
}

pub struct Repository {
    conn: DatabaseConnection,
}

impl Repository {
    pub async fn new(db_url: &str) -> Result<Self, RepositoryError> {
        let db = Database::connect(db_url).await?;
        Ok(Self { conn: db })
    }

    pub fn new_with_db(db: DatabaseConnection) -> Self {
        Self { conn: db }
    }

    async fn _create_index(
        &self,
        tx: DatabaseTransaction,
        embedding_model: String,
        index_params: CreateIndexParams,
        vectordb: vectordbs::VectorDBTS,
        text_splitter: String,
        repository_id: String,
    ) -> Result<DatabaseTransaction, RepositoryError> {
        let mut unique_params = None;
        if let Some(u_params) = &index_params.unique_params {
            unique_params.replace(serde_json::to_string(u_params)?);
        }
        let index = entity::index::ActiveModel {
            name: Set(index_params.name.clone()),
            embedding_model: Set(embedding_model),
            text_splitter: Set(text_splitter),
            vector_db: Set(vectordb.name()),
            vector_db_params: NotSet,
            unique_params: Set(unique_params),
            repository_id: Set(repository_id),
        };
        let insert_result = IndexEntity::insert(index)
            .on_conflict(
                OnConflict::column(entity::index::Column::Name)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&tx)
            .await;
        if let Err(err) = insert_result {
            if err != DbErr::RecordNotInserted {
                tx.rollback().await?;
                return Err(RepositoryError::DatabaseError(err));
            }
        }
        if let Err(err) = vectordb.create_index(index_params.clone()).await {
            tx.rollback().await?;
            return Err(RepositoryError::VectorDb(err));
        }

        Ok(tx)
    }

    pub async fn create_index(
        &self,
        embedding_model: String,
        index_params: CreateIndexParams,
        vectordb: vectordbs::VectorDBTS,
        text_splitter: String,
        repository_name: &str,
    ) -> Result<(), RepositoryError> {
        let tx = self.conn.begin().await?;
        let tx = self
            ._create_index(
                tx,
                embedding_model,
                index_params,
                vectordb,
                text_splitter,
                repository_name.into(),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_index(&self, index: &str) -> Result<IndexModel, RepositoryError> {
        IndexEntity::find()
            .filter(index::Column::Name.eq(index))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::IndexNotFound(index.into()))
    }

    pub async fn get_texts_for_memory_session(
        &self,
        repository_name: &str,
        session_id: &str,
    ) -> Result<Vec<Text>, RepositoryError> {
        let contents = entity::content::Entity::find()
            .filter(entity::content::Column::RepositoryId.eq(repository_name))
            .filter(entity::content::Column::MemorySessionId.eq(session_id))
            .all(&self.conn)
            .await?;
        let mut texts = Vec::new();
        for content in contents {
            let metadata: HashMap<String, serde_json::Value> = content
                .metadata
                .map(|s| serde_json::from_str(&s).unwrap())
                .unwrap_or_default();
            texts.push(Text {
                text: content.text,
                metadata,
            });
        }
        Ok(texts)
    }

    pub async fn add_text_to_repo(
        &self,
        repository_name: &str,
        texts: Vec<Text>,
        memory_session: Option<&str>,
    ) -> Result<(), RepositoryError> {
        let tx = self.conn.begin().await?;
        let mut content_list = Vec::new();
        for text in texts {
            let meta = serde_json::to_string(&text.metadata)?;
            let content_id = create_content_id(repository_name, &text.text);
            let content_type = match memory_session {
                Some(_) => ContentType::Memory,
                None => ContentType::Text,
            };
            content_list.push(entity::content::ActiveModel {
                id: Set(content_id),
                repository_id: Set(repository_name.into()),
                memory_session_id: Set(memory_session.map(|s| s.into())),
                text: Set(text.text),
                metadata: Set(Some(meta)),
                content_type: Set(content_type.to_string()),
                extractors_state: Set(Some(json!(ExtractorsState::default()).to_string())),
            });
        }
        let _ = entity::content::Entity::insert_many(content_list)
            .on_conflict(
                OnConflict::column(entity::content::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&tx)
            .await;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_chunks(
        &self,
        content_id: &str,
        chunks: Vec<Chunk>,
        index_name: &str,
    ) -> Result<(), RepositoryError> {
        let tx = self.conn.begin().await?;
        let chunk_models: Vec<entity::index_chunks::ActiveModel> = chunks
            .iter()
            .map(|chunk| entity::index_chunks::ActiveModel {
                chunk_id: Set(chunk.chunk_id.clone()),
                content_id: Set(content_id.into()),
                text: Set(chunk.text.clone()),
                index_name: Set(index_name.into()),
            })
            .collect();
        let _ = entity::index_chunks::Entity::insert_many(chunk_models)
            .on_conflict(
                OnConflict::column(entity::index_chunks::Column::ChunkId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&tx)
            .await;

        // Mark the content as indexed
        let content_entity = entity::content::Entity::find()
            .filter(entity::content::Column::Id.eq(content_id))
            .one(&tx)
            .await?
            .ok_or(RepositoryError::ContentNotFound(content_id.to_string()))?;
        let mut extractors_state: ExtractorsState = serde_json::from_str(
            &content_entity
                .extractors_state
                .clone()
                .unwrap_or("{}".into()),
        )
        .map_err(|e| RepositoryError::LogicError(e.to_string()))?;
        extractors_state.update(index_name);
        let extractor_state_str = serde_json::to_string(&extractors_state)
            .map_err(|e| RepositoryError::LogicError(e.to_string()))?;
        let mut content_entity: entity::content::ActiveModel = content_entity.into();
        content_entity.extractors_state = Set(Some(extractor_state_str));
        content_entity.update(&tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn chunk_with_id(&self, id: &str) -> Result<ChunkWithMetadata, RepositoryError> {
        let chunk = entity::index_chunks::Entity::find()
            .filter(entity::index_chunks::Column::ChunkId.eq(id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::ChunkNotFound(id.to_string()))?;
        let content = entity::content::Entity::find()
            .filter(entity::content::Column::Id.eq(&chunk.content_id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::ContentNotFound(
                chunk.content_id.to_string(),
            ))?;
        Ok(ChunkWithMetadata {
            chunk_id: chunk.chunk_id,
            text: chunk.text,
            metadata: content
                .metadata
                .map(|s| serde_json::from_str(&s).unwrap())
                .unwrap_or_default(),
        })
    }

    pub async fn content_with_unapplied_extractor(
        &self,
        repo_id: &str,
        extractor: &str,
        extractor_content_type: ContentType,
    ) -> Result<Vec<entity::content::Model>, RepositoryError> {
        let result = entity::content::Entity::find()
            .filter(entity::content::Column::RepositoryId.eq(repo_id))
            .filter(entity::content::Column::ExtractorsState.not_like(&format!("%{}%", extractor)))
            .filter(entity::content::Column::ContentType.eq(extractor_content_type.to_string()))
            .all(&self.conn)
            .await?;
        Ok(result)
    }

    pub async fn create_memory_session(
        &self,
        session_id: &str,
        repo_id: &str,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<(), RepositoryError> {
        let tx = self.conn.begin().await?;
        let metadata = Some(json!(metadata).to_string());
        let memory_session = entity::memory_sessions::ActiveModel {
            session_id: Set(session_id.to_string()),
            repository_id: Set(repo_id.into()),
            metadata: Set(metadata),
        };
        let _ = entity::memory_sessions::Entity::insert(memory_session)
            .on_conflict(
                OnConflict::column(entity::memory_sessions::Column::SessionId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&tx)
            .await;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_repository(
        &self,
        repository: DataRepository,
    ) -> Result<(), RepositoryError> {
        let tx = self.conn.begin().await?;
        let repository = entity::data_repository::ActiveModel {
            name: Set(repository.name),
            extractors: Set(Some(serde_json::to_string(&repository.extractors)?)),
            metadata: Set(Some(serde_json::to_string(&repository.metadata)?)),
            data_connectors: Set(Some(serde_json::to_string(&repository.data_connectors)?)),
        };
        let _ = DataRepositoryEntity::insert(repository)
            .on_conflict(
                OnConflict::column(entity::data_repository::Column::Name)
                    .update_columns(vec![
                        entity::data_repository::Column::Extractors,
                        entity::data_repository::Column::Metadata,
                    ])
                    .to_owned(),
            )
            .exec(&tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn repositories(&self) -> Result<Vec<DataRepository>, RepositoryError> {
        let repository_models: Vec<DataRepository> = DataRepositoryEntity::find()
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect();
        Ok(repository_models)
    }

    pub async fn repository_by_name(&self, name: &str) -> Result<DataRepository, RepositoryError> {
        let repository_model = DataRepositoryEntity::find()
            .filter(entity::data_repository::Column::Name.eq(name))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(name.to_owned()))?;
        Ok(repository_model.into())
    }
}

fn create_content_id(index_name: &str, text: &str) -> String {
    let mut s = DefaultHasher::new();
    index_name.hash(&mut s);
    text.hash(&mut s);
    format!("{:x}", s.finish())
}
