use nanoid::nanoid;
use sea_orm::ConnectionTrait;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::SystemTime;
use tracing::info;

use anyhow::Result;
use entity::data_repository::Entity as DataRepositoryEntity;
use entity::extraction_event::Entity as ExtractionEventEntity;
use entity::index::Entity as IndexEntity;
use entity::index::Model as IndexModel;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveModelTrait, ColumnTrait, DbBackend, Statement};
use sea_orm::{
    ActiveValue::NotSet, Database, DatabaseConnection, DbErr, EntityTrait, Set, TransactionTrait,
};
use sea_orm::{ConnectOptions, QueryFilter};
use sea_query::expr::Expr;
use serde::{Deserialize, Serialize};
use serde_json::json;
use smart_default::SmartDefault;
use strum_macros::{Display, EnumString};
use thiserror::Error;

use crate::entity::{index, work};
use crate::text_splitters::TextSplitterKind;
use crate::vectordbs::{self, CreateIndexParams};
use crate::{entity, vectordbs::IndexDistance};
use entity::work::Entity as WorkEntity;

#[derive(Serialize, Deserialize, Display, EnumString)]
pub enum ExtractionEventPayload {
    SyncRepository { memory_session: Option<String> },
    CreateContent { content_id: String },
}

#[derive(Serialize, Deserialize)]
pub struct ExtractionEvent {
    pub id: String,
    pub repository_id: String,
    pub payload: ExtractionEventPayload,
}

#[derive(Serialize, Deserialize, Default)]
struct ExtractorsState {
    #[serde(default)]
    state: HashMap<String, u64>,
}

#[derive(Clone, Error, Debug, Display, EnumString, Serialize, Deserialize, SmartDefault)]
pub enum ContentType {
    #[strum(serialize = "text")]
    #[default]
    Text,
}

#[derive(Debug, Clone)]
pub struct Text {
    pub id: String,
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Text {
    pub fn from_text(
        repository: &str,
        text: &str,
        memory_session: Option<&str>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Self {
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        if let Some(sess) = memory_session {
            sess.hash(&mut s);
        }
        text.hash(&mut s);
        let id = format!("{:x}", s.finish());
        Self {
            id,
            text: text.into(),
            metadata,
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, EnumString, Display)]
#[serde(rename = "extractor_filter")]
pub enum ExtractorFilter {
    MemorySession { session_id: String },
    ContentType { content_type: ContentType },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "extractor")]
pub struct ExtractorConfig {
    pub name: String,
    pub extractor_type: ExtractorType,
    pub filter: ExtractorFilter,
}

impl Default for ExtractorConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            filter: ExtractorFilter::ContentType {
                content_type: ContentType::Text,
            },
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
            .map(|s| serde_json::from_value(s).unwrap())
            .unwrap_or_default();
        let data_connectors = model
            .data_connectors
            .map(|s| serde_json::from_value(s).unwrap())
            .unwrap_or_default();
        let metadata = model
            .metadata
            .map(|s| serde_json::from_value(s).unwrap())
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
    pub content_id: String,
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

#[derive(
    Debug, PartialEq, Eq, Serialize, Clone, Deserialize, EnumString, Display, SmartDefault,
)]
pub enum WorkState {
    #[default]
    Unknown,
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Work {
    pub id: String,
    pub content_id: String,
    pub repository_id: String,
    pub extractor: String,
    pub work_state: WorkState,
    pub worker_id: Option<String>,
}

impl Work {
    pub fn new(
        content_id: &str,
        repository: &str,
        extractor: &str,
        worker_id: Option<&str>,
    ) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        repository.hash(&mut s);
        extractor.hash(&mut s);
        let id = format!("{:x}", s.finish());

        Self {
            id,
            content_id: content_id.into(),
            repository_id: repository.into(),
            extractor: extractor.into(),
            work_state: WorkState::Pending,
            worker_id: worker_id.map(|w| w.into()),
        }
    }

    pub fn terminal_state(&self) -> bool {
        self.work_state == WorkState::Completed || self.work_state == WorkState::Failed
    }
}

impl From<work::Model> for Work {
    fn from(model: work::Model) -> Self {
        Self {
            id: model.id,
            content_id: model.content_id,
            repository_id: model.repository_id,
            extractor: model.extractor,
            work_state: WorkState::from_str(&model.state).unwrap(),
            worker_id: model.worker_id,
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
        let mut opt = ConnectOptions::new(db_url.to_owned());
        opt.sqlx_logging(false); // Disabling SQLx log;

        let db = Database::connect(opt).await?;
        Ok(Self { conn: db })
    }

    pub fn new_with_db(db: DatabaseConnection) -> Self {
        Self { conn: db }
    }

    pub async fn create_index(
        &self,
        embedding_model: String,
        index_params: CreateIndexParams,
        vectordb: vectordbs::VectorDBTS,
        text_splitter: String,
        repository_name: &str,
    ) -> Result<(), RepositoryError> {
        let index = entity::index::ActiveModel {
            name: Set(index_params.name.clone()),
            embedding_model: Set(embedding_model),
            text_splitter: Set(text_splitter),
            vector_db: Set(vectordb.name()),
            vector_db_params: NotSet,
            unique_params: Set(index_params.unique_params.clone().map(|v| json!(v))),
            repository_id: Set(repository_name.into()),
        };

        self.conn
            .transaction::<_, (), RepositoryError>(|txn| {
                Box::pin(async move {
                    let insert_result = IndexEntity::insert(index)
                        .on_conflict(
                            OnConflict::column(entity::index::Column::Name)
                                .do_nothing()
                                .to_owned(),
                        )
                        .exec(txn)
                        .await;
                    if let Err(err) = insert_result {
                        if err != DbErr::RecordNotInserted {
                            return Err(RepositoryError::DatabaseError(err));
                        }
                    }
                    if let Err(err) = vectordb.create_index(index_params.clone()).await {
                        return Err(RepositoryError::VectorDb(err));
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|e| RepositoryError::LogicError(e.to_string()))?;
        Ok(())
    }

    pub async fn get_index(&self, index: &str) -> Result<IndexModel, RepositoryError> {
        IndexEntity::find()
            .filter(index::Column::Name.eq(index))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::IndexNotFound(index.into()))
    }

    pub async fn create_memory_session(
        &self,
        session_id: &str,
        repo_id: &str,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<(), RepositoryError> {
        let memory_session = entity::memory_sessions::ActiveModel {
            session_id: Set(session_id.to_string()),
            repository_id: Set(repo_id.into()),
            metadata: Set(Some(json!(metadata))),
        };
        let _ = entity::memory_sessions::Entity::insert(memory_session)
            .on_conflict(
                OnConflict::column(entity::memory_sessions::Column::SessionId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn retrieve_messages_from_memory(
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
                .map(|s| serde_json::from_value(s).unwrap())
                .unwrap_or_default();
            texts.push(Text {
                text: content.text,
                id: content.id,
                metadata,
            });
        }
        Ok(texts)
    }

    pub async fn add_content(
        &self,
        repository_name: &str,
        texts: Vec<Text>,
        memory_session: Option<&str>,
    ) -> Result<(), RepositoryError> {
        if memory_session.is_some() {
            // Ensure that the session exists
            let _session = entity::memory_sessions::Entity::find()
                .filter(entity::memory_sessions::Column::SessionId.eq(memory_session))
                .one(&self.conn)
                .await?
                .ok_or(RepositoryError::SessionNotFound("session not found".into()))?;
        }
        let mut content_list = Vec::new();
        let mut extraction_events = Vec::new();
        for text in texts {
            info!("adding text: {}", &text.id);
            content_list.push(entity::content::ActiveModel {
                id: Set(text.id.clone()),
                repository_id: Set(repository_name.into()),
                memory_session_id: Set(memory_session.map(|s| s.into())),
                text: Set(text.text),
                metadata: Set(Some(json!(text.metadata))),
                content_type: Set(ContentType::Text.to_string()),
                extractors_state: Set(Some(json!(ExtractorsState::default()))),
            });
            let extraction_event = ExtractionEvent {
                id: nanoid!(),
                repository_id: repository_name.into(),
                payload: ExtractionEventPayload::CreateContent {
                    content_id: text.id.clone(),
                },
            };
            extraction_events.push(entity::extraction_event::ActiveModel {
                id: Set(extraction_event.id.clone()),
                payload: Set(json!(extraction_event)),
                allocation_info: NotSet,
                processed_at: NotSet,
            });
        }

        self.conn
            .transaction::<_, (), RepositoryError>(|txn| {
                Box::pin(async move {
                    let result = entity::content::Entity::insert_many(content_list)
                        .on_conflict(
                            OnConflict::column(entity::content::Column::Id)
                                .do_nothing()
                                .to_owned(),
                        )
                        .exec(txn)
                        .await;
                    if let Err(err) = result {
                        if err == DbErr::RecordNotInserted {
                            return Ok(());
                        }
                        return Err(RepositoryError::DatabaseError(err));
                    }
                    let _ = ExtractionEventEntity::insert_many(extraction_events)
                        .exec(txn)
                        .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| RepositoryError::LogicError(e.to_string()))?;
        Ok(())
    }

    pub async fn content_from_repo(
        &self,
        content_id: &str,
        repo_id: &str,
    ) -> Result<entity::content::Model, RepositoryError> {
        let model = entity::content::Entity::find()
            .filter(entity::content::Column::RepositoryId.eq(repo_id))
            .filter(entity::content::Column::Id.eq(content_id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::ContentNotFound(content_id.to_owned()))?;
        Ok(model)
    }

    pub async fn content_with_unapplied_extractor(
        &self,
        repo_id: &str,
        extractor: &ExtractorConfig,
        content_id: Option<&str>,
    ) -> Result<Vec<entity::content::Model>, RepositoryError> {
        let mut values = vec![repo_id.into(), extractor.name.clone().into()];
        let mut query: String = "select * from content where repository_id=$1".to_string();
        let mut idx = 3;
        if let Some(content_id) = content_id {
            values.push(content_id.into());
            query.push_str(format!(" and id = ${}", idx).as_str());
            idx += 1;
        }
        match &extractor.filter {
            ExtractorFilter::MemorySession { session_id } => {
                values.push(session_id.into());
                query.push_str(format!(" and memory_session_id = ${}", idx).as_str());
            }
            ExtractorFilter::ContentType { content_type } => {
                values.push(content_type.to_string().into());
                query.push_str(
                    format!(" and content_type = ${} and memory_session_id is NULL", idx).as_str(),
                );
            }
        }
        query.push_str(" and COALESCE(cast(extractors_state->'state'->>$2 as int),0) < 1");
        let result = entity::content::Entity::find()
            .from_raw_sql(Statement::from_sql_and_values(
                DbBackend::Postgres,
                &query,
                values,
            ))
            .all(&self.conn)
            .await?;
        Ok(result)
    }

    pub async fn mark_content_as_processed(
        &self,
        content_id: &str,
        extractor: &str,
    ) -> Result<(), anyhow::Error> {
        let content_id = content_id.to_string();
        let extractor = extractor.to_string();
        // TODO change the '1' to a timestamp so that the state value reflects
        // when was the worker state updated.
        let query = r#"update content set extractors_state['state'][$2] = '1' where id=$1"#;
        let values = vec![content_id.into(), extractor.clone().into()];
        let _ = self
            .conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                values,
            ))
            .await?;
        Ok(())
    }

    pub async fn unprocessed_extraction_events(
        &self,
    ) -> Result<Vec<ExtractionEvent>, anyhow::Error> {
        let extraction_events = ExtractionEventEntity::find()
            .filter(entity::extraction_event::Column::ProcessedAt.is_null())
            .all(&self.conn)
            .await?;
        let mut events = Vec::new();
        for e in &extraction_events {
            let event: ExtractionEvent = serde_json::from_value(e.payload.clone())?;
            events.push(event);
        }
        Ok(events)
    }

    pub async fn mark_extraction_event_as_processed(
        &self,
        extraction_id: &str,
    ) -> Result<(), anyhow::Error> {
        let extraction_event = ExtractionEventEntity::find()
            .filter(entity::extraction_event::Column::Id.eq(extraction_id))
            .one(&self.conn)
            .await?
            .unwrap();
        let mut extraction_event: entity::extraction_event::ActiveModel = extraction_event.into();
        extraction_event.processed_at = Set(Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        ));
        extraction_event.update(&self.conn).await?;
        Ok(())
    }

    pub async fn create_chunks(
        &self,
        content_id: &str,
        chunks: Vec<Chunk>,
        index_name: &str,
    ) -> Result<(), RepositoryError> {
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
            .exec(&self.conn)
            .await?;
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
            content_id: chunk.content_id,
            text: chunk.text,
            metadata: content
                .metadata
                .map(|s| serde_json::from_value(s).unwrap())
                .unwrap_or_default(),
        })
    }

    pub async fn upsert_repository(
        &self,
        repository: DataRepository,
    ) -> Result<(), RepositoryError> {
        let extractor_event = ExtractionEvent {
            id: nanoid!(),
            repository_id: repository.name.clone(),
            payload: ExtractionEventPayload::SyncRepository {
                memory_session: None,
            },
        };
        let repository_model = entity::data_repository::ActiveModel {
            name: Set(repository.name),
            extractors: Set(Some(json!(repository.extractors))),
            metadata: Set(Some(json!(repository.metadata))),
            data_connectors: Set(Some(json!(repository.data_connectors))),
        };
        let extraction_event_model = entity::extraction_event::ActiveModel {
            id: Set(extractor_event.id.clone()),
            payload: Set(json!(extractor_event)),
            allocation_info: NotSet,
            processed_at: NotSet,
        };
        let _ = self
            .conn
            .transaction::<_, (), RepositoryError>(|txn| {
                Box::pin(async move {
                    let _ = DataRepositoryEntity::insert(repository_model)
                        .on_conflict(
                            OnConflict::column(entity::data_repository::Column::Name)
                                .update_columns(vec![
                                    entity::data_repository::Column::Extractors,
                                    entity::data_repository::Column::Metadata,
                                ])
                                .to_owned(),
                        )
                        .exec(txn)
                        .await?;
                    let _ = ExtractionEventEntity::insert(extraction_event_model)
                        .exec(txn)
                        .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|e| RepositoryError::LogicError(e.to_string()));

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

    pub async fn get_extractor(
        &self,
        repo_id: &str,
        extractor_name: &str,
    ) -> Result<ExtractorConfig, RepositoryError> {
        let extractors_json = DataRepositoryEntity::find()
            .filter(entity::data_repository::Column::Name.eq(repo_id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(repo_id.to_owned()))?
            .extractors
            .ok_or(RepositoryError::RepositoryNotFound(repo_id.to_owned()))?;
        let extractor_config: Vec<ExtractorConfig> = serde_json::from_value(extractors_json)
            .map_err(|e| RepositoryError::LogicError(e.to_string()))?;
        let extractor = extractor_config
            .iter()
            .find(|e| e.name == extractor_name)
            .ok_or(RepositoryError::LogicError(extractor_name.to_owned()))?;
        Ok(extractor.to_owned())
    }

    pub async fn insert_work(&self, work: &Work) -> Result<(), RepositoryError> {
        let work_model = entity::work::ActiveModel {
            id: Set(work.id.clone()),
            state: Set(work.work_state.to_string()),
            worker_id: Set(work.worker_id.as_ref().map(|id| id.to_owned())),
            content_id: Set(work.content_id.clone()),
            extractor: Set(work.extractor.clone()),
            repository_id: Set(work.repository_id.clone()),
        };
        WorkEntity::insert(work_model).exec(&self.conn).await?;
        Ok(())
    }

    pub async fn unallocated_work(&self) -> Result<Vec<work::Model>, RepositoryError> {
        let work_models = WorkEntity::find()
            .filter(entity::work::Column::WorkerId.is_null())
            .filter(entity::work::Column::State.eq(WorkState::Pending.to_string()))
            .all(&self.conn)
            .await?;
        Ok(work_models)
    }

    pub async fn assign_work(
        &self,
        allocation: HashMap<String, String>,
    ) -> Result<(), RepositoryError> {
        for (work_id, executor_id) in allocation.iter() {
            WorkEntity::update_many()
                .col_expr(entity::work::Column::WorkerId, Expr::value(executor_id))
                .filter(entity::work::Column::Id.eq(work_id))
                .exec(&self.conn)
                .await?;
        }
        Ok(())
    }

    pub async fn update_work_state(
        &self,
        work_id: &str,
        state: WorkState,
    ) -> Result<(), RepositoryError> {
        entity::work::Entity::update_many()
            .col_expr(entity::work::Column::State, Expr::value(state.to_string()))
            .filter(entity::work::Column::Id.eq(work_id))
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    pub async fn work_for_worker(&self, worker_id: &str) -> Result<Vec<Work>, RepositoryError> {
        let work_models = WorkEntity::find()
            .filter(entity::work::Column::WorkerId.eq(worker_id))
            .filter(entity::work::Column::State.eq(WorkState::Pending.to_string()))
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|m| m.into())
            .collect();
        Ok(work_models)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::db_utils::create_db;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_extractors_for_repository() {
        let extractor1 = ExtractorConfig {
            name: "extractor1".into(),
            extractor_type: ExtractorType::Embedding {
                model: "model1".into(),
                text_splitter: TextSplitterKind::NewLine,
                distance: IndexDistance::Cosine,
            },
            filter: ExtractorFilter::ContentType {
                content_type: ContentType::Text,
            },
        };
        let extractor2 = ExtractorConfig {
            name: "extractor2".into(),
            extractor_type: ExtractorType::Embedding {
                model: "model2".into(),
                text_splitter: TextSplitterKind::NewLine,
                distance: IndexDistance::Cosine,
            },
            filter: ExtractorFilter::MemorySession {
                session_id: "abcd".into(),
            },
        };
        let repo = DataRepository {
            name: "test".to_owned(),
            data_connectors: vec![],
            extractors: vec![extractor1.clone(), extractor2.clone()],
            metadata: HashMap::new(),
        };

        let db = create_db().await.unwrap();
        let repository = Repository::new_with_db(db);
        repository.upsert_repository(repo.clone()).await.unwrap();

        repository
            .add_content(
                &repo.name,
                vec![
                    Text::from_text(&repo.name, "hello", None, HashMap::new()),
                    Text::from_text(&repo.name, "world", None, HashMap::new()),
                ],
                None,
            )
            .await
            .unwrap();

        let memory_session_id = "abcd";

        repository
            .create_memory_session(memory_session_id, &repo.name, HashMap::new())
            .await
            .unwrap();

        repository
            .add_content(
                &repo.name,
                vec![
                    Text::from_text(
                        &repo.name,
                        "hello ai",
                        Some(memory_session_id),
                        HashMap::new(),
                    ),
                    Text::from_text(
                        &repo.name,
                        "hello human",
                        Some(memory_session_id),
                        HashMap::new(),
                    ),
                ],
                Some("abcd"),
            )
            .await
            .unwrap();

        let content_list1 = repository
            .content_with_unapplied_extractor(&repo.name, &extractor1, None)
            .await
            .unwrap();
        assert_eq!(2, content_list1.len());

        let content_list2 = repository
            .content_with_unapplied_extractor(&repo.name, &extractor2, None)
            .await
            .unwrap();
        assert_eq!(2, content_list2.len());
    }
}
