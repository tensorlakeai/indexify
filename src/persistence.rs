use nanoid::nanoid;
use sea_orm::sea_query::Expr;
use sea_orm::{ConnectionTrait, QueryTrait};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info};

use anyhow::{anyhow, Result};
use entity::data_repository::Entity as DataRepositoryEntity;
use entity::extraction_event::Entity as ExtractionEventEntity;
use entity::extractors;
use entity::index::Entity as IndexEntity;
use entity::index::Model as IndexModel;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveModelTrait, ColumnTrait, DbBackend, Statement};
use sea_orm::{
    ActiveValue::NotSet, Database, DatabaseConnection, DbErr, EntityTrait, Set, TransactionTrait,
};
use sea_orm::{ConnectOptions, QueryFilter};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smart_default::SmartDefault;
use strum_macros::{Display, EnumString};
use thiserror::Error;

use crate::entity::{index, work};
use crate::vectordbs::{self};
use crate::{entity, vectordbs::IndexDistance};
use entity::work::Entity as WorkEntity;

pub struct Index {
    pub name: String,
    pub schema: ExtractorOutputSchema,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexBindings {
    // feature_name -> index_name
    bindings: HashMap<String, String>,
}

impl IndexBindings {
    pub fn from_feature_to_index_names(bindings: HashMap<String, String>) -> Self {
        Self { bindings }
    }

    pub fn index_names(&self) -> Vec<String> {
        self.bindings.values().cloned().collect()
    }

    pub fn get_index_name(&self, feature_name: &str) -> Option<&String> {
        self.bindings.get(feature_name)
    }

    pub fn bindings(&self) -> HashMap<String, String> {
        self.bindings.clone()
    }

    pub fn hash(&self, mut hasher: &mut DefaultHasher) {
        for (feature_name, index_name) in &self.bindings {
            feature_name.hash(&mut hasher);
            index_name.hash(&mut hasher);
        }
    }

    #[cfg(test)]
    pub fn from_feature(feature: &str) -> Self {
        Self {
            bindings: HashMap::from([(feature.into(), feature.into())]),
        }
    }
}

impl std::fmt::Display for IndexBindings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bindings = Vec::new();
        for (feature_name, index_name) in &self.bindings {
            bindings.push(format!("{}:{}", feature_name, index_name));
        }
        write!(f, "{}", bindings.join(","))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorBinding {
    pub id: String,
    pub extractor_name: String,
    pub indexes: IndexBindings,
    pub filters: Vec<ExtractorFilter>,
    pub input_params: serde_json::Value,
}

impl ExtractorBinding {
    pub fn new(
        repository: &str,
        extractor_name: String,
        indexes: IndexBindings,
        filters: Vec<ExtractorFilter>,
        input_params: serde_json::Value,
    ) -> ExtractorBinding {
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        extractor_name.hash(&mut s);
        indexes.hash(&mut s);
        let id = format!("{:x}", s.finish());
        ExtractorBinding {
            id,
            extractor_name,
            indexes,
            filters,
            input_params,
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Display, EnumString)]
pub enum ExtractionEventPayload {
    ExtractorBindingAdded { repository: String, id: String },
    CreateContent { content_id: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractionEvent {
    pub id: String,
    pub repository_id: String,
    pub payload: ExtractionEventPayload,
}

#[derive(Serialize, Deserialize, Default)]
struct ExtractorBindingsState {
    #[serde(default)]
    state: HashMap<String, u64>,
}

#[derive(Clone, Error, Debug, Display, EnumString, Serialize, Deserialize, SmartDefault)]
pub enum ContentType {
    #[strum(serialize = "text")]
    #[default]
    Text,

    #[strum(serialize = "pdf")]
    Pdf,
}

#[derive(Clone, Error, Debug, Display, EnumString, Serialize, Deserialize, SmartDefault)]
pub enum PayloadType {
    #[strum(serialize = "embedded_storage")]
    #[default]
    EmbeddedStorage,

    #[strum(serialize = "blob_storage_link")]
    BlobStorageLink,
}

#[derive(Debug, Clone)]
pub struct ContentPayload {
    pub id: String,
    pub content_type: ContentType,
    pub payload: String,
    pub payload_type: PayloadType,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ContentPayload {
    pub fn from_text(
        repository: &str,
        text: &str,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Self {
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        text.hash(&mut s);
        let id = format!("{:x}", s.finish());
        Self {
            id,
            content_type: ContentType::Text,
            payload: text.into(),
            payload_type: PayloadType::EmbeddedStorage,
            metadata,
        }
    }

    pub fn from_file(repository: &str, name: &str, path: &str) -> Self {
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        // TODO remove hardcoding of pdf with some thing thats
        // parameterized.
        Self {
            id,
            content_type: ContentType::Pdf,
            payload: path.into(),
            payload_type: PayloadType::BlobStorageLink,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display)]
#[serde(rename = "extractor_type")]
pub enum ExtractorOutputSchema {
    #[serde(rename = "embedding")]
    Embedding {
        dim: usize,
        distance_metric: IndexDistance,
    },

    #[serde(rename = "attributes")]
    Attributes(serde_json::Value),
}

impl ExtractorOutputSchema {
    #[cfg(test)]
    pub fn embedding(dim: usize, distance_metric: IndexDistance) -> Self {
        Self::Embedding {
            dim,
            distance_metric,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub outputs: HashMap<String, ExtractorOutputSchema>,
}

impl ExtractorSchema {
    #[cfg(test)]
    pub fn from_output_schema(name: &str, schema: ExtractorOutputSchema) -> Self {
        let output_schemas = HashMap::from([(name.into(), schema)]);
        Self {
            outputs: output_schemas,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumString, Display)]
#[serde(rename = "extractor_filter")]
pub enum ExtractorFilter {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Neq {
        field: String,
        value: serde_json::Value,
    },
}

#[derive(Debug, Clone)]
pub struct ExtractorDescription {
    pub name: String,
    pub description: String,
    pub input_params: serde_json::Value,
    pub schemas: ExtractorSchema,
}

impl From<extractors::Model> for ExtractorDescription {
    fn from(model: extractors::Model) -> Self {
        // TODO remove unwrap()
        let output_schema = serde_json::from_value(model.output_schema).unwrap();
        Self {
            name: model.id,
            description: model.description,
            input_params: model.input_params,
            schemas: output_schema,
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
    pub extractor_bindings: Vec<ExtractorBinding>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<entity::data_repository::Model> for DataRepository {
    fn from(model: entity::data_repository::Model) -> Self {
        let extractors = model
            .extractor_bindings
            .map(|s| {
                let eb_hash: HashMap<String, ExtractorBinding> = serde_json::from_value(s).unwrap();
                eb_hash.values().cloned().collect()
            })
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
            extractor_bindings: extractors,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedAttributes {
    pub id: String,
    pub content_id: String,
    pub attributes: serde_json::Value,
    pub extractor_name: String,
}

impl ExtractedAttributes {
    pub fn new(content_id: &str, attributes: serde_json::Value, extractor_name: &str) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        extractor_name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        Self {
            id,
            content_id: content_id.into(),
            attributes,
            extractor_name: extractor_name.into(),
        }
    }
}

impl From<entity::attributes_index::Model> for ExtractedAttributes {
    fn from(model: entity::attributes_index::Model) -> Self {
        Self {
            id: model.id,
            content_id: model.content_id,
            attributes: model.data,
            extractor_name: model.extractor_id,
        }
    }
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Event {
    pub id: String,
    pub message: String,
    pub unix_timestamp: u64,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Event {
    pub fn new(
        message: &str,
        unix_timestamp: Option<u64>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Self {
        let id = nanoid!();
        let unix_timestamp = unix_timestamp.unwrap_or(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        Self {
            id,
            message: message.into(),
            unix_timestamp,
            metadata,
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
    pub indexes: IndexBindings,
    pub extractor: String,
    pub extractor_params: serde_json::Value,
    pub work_state: WorkState,
    pub executor_id: Option<String>,
}

impl Work {
    pub fn new(
        content_id: &str,
        repository: &str,
        indexes: IndexBindings,
        extractor: &str,
        extractor_params: &serde_json::Value,
        worker_id: Option<&str>,
    ) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        repository.hash(&mut s);
        indexes.hash(&mut s);
        extractor.hash(&mut s);
        let id = format!("{:x}", s.finish());

        Self {
            id,
            content_id: content_id.into(),
            repository_id: repository.into(),
            indexes,
            extractor: extractor.into(),
            extractor_params: extractor_params.clone(),
            work_state: WorkState::Pending,
            executor_id: worker_id.map(|w| w.into()),
        }
    }
}

impl TryFrom<work::Model> for Work {
    type Error = anyhow::Error;

    fn try_from(model: work::Model) -> Result<Self, anyhow::Error> {
        let indexes: IndexBindings =
            serde_json::from_value(model.index_names).map_err(|e| anyhow!(e.to_string()))?;
        Ok(Self {
            id: model.id,
            content_id: model.content_id,
            repository_id: model.repository_id,
            indexes,
            extractor: model.extractor,
            extractor_params: model.extractor_params,
            work_state: WorkState::from_str(&model.state).unwrap(),
            executor_id: model.worker_id,
        })
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

    #[error("extractor`{0}` not found")]
    ExtractorNotFound(String),

    #[error("index `{0}` not found")]
    IndexNotFound(String),

    #[error("content`{0}` not found")]
    ContentNotFound(String),

    #[error("chunk `{0}` not found")]
    ChunkNotFound(String),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),

    #[error("internal application error `{0}`")]
    LogicError(String),
}

#[derive(Debug)]
pub struct Repository {
    conn: DatabaseConnection,
}

impl Repository {
    pub async fn new(db_url: &str) -> Result<Self, RepositoryError> {
        let mut opt = ConnectOptions::new(db_url.to_owned());
        opt.sqlx_logging(false); // Disabling SQLx log;
        info!("connecting to db: {}", db_url);
        let conn = Database::connect(opt).await?;
        Ok(Self { conn })
    }

    pub fn new_with_db(conn: DatabaseConnection) -> Self {
        Self { conn }
    }

    #[tracing::instrument]
    pub fn get_db_conn_clone(&self) -> DatabaseConnection {
        self.conn.clone()
    }

    #[tracing::instrument]
    pub async fn create_index_metadata(
        &self,
        repository: &str,
        extractor_name: &str,
        index_name: &str,
        storage_index_name: &str,
        index_schema: serde_json::Value,
        index_type: &str,
    ) -> Result<(), RepositoryError> {
        let index = entity::index::ActiveModel {
            name: Set(index_name.into()),
            vector_index_name: Set(Some(storage_index_name.into())),
            extractor_name: Set(extractor_name.into()),
            index_type: Set(index_type.into()),
            index_schema: Set(index_schema),
            repository_id: Set(repository.into()),
        };
        let insert_result = IndexEntity::insert(index)
            .on_conflict(
                OnConflict::column(entity::index::Column::Name)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&self.conn)
            .await;
        if let Err(err) = insert_result {
            if err != DbErr::RecordNotInserted {
                return Err(RepositoryError::DatabaseError(err));
            }
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_indexes(&self, repository: &str) -> Result<Vec<Index>, RepositoryError> {
        let index_models = IndexEntity::find()
            .filter(index::Column::RepositoryId.eq(repository))
            .all(&self.conn)
            .await
            .map_err(RepositoryError::DatabaseError)?;
        let indexes: Vec<Index> = index_models
            .into_iter()
            .map(|i| Index {
                name: i.name,
                schema: serde_json::from_value(i.index_schema).unwrap(),
            })
            .collect();
        Ok(indexes)
    }

    #[tracing::instrument]
    pub async fn get_index(
        &self,
        index: &str,
        repository: &str,
    ) -> Result<IndexModel, RepositoryError> {
        IndexEntity::find()
            .filter(index::Column::Name.eq(index))
            .filter(index::Column::RepositoryId.eq(repository))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::IndexNotFound(index.into()))
    }

    #[tracing::instrument]
    pub async fn add_events(
        &self,
        repository: &str,
        events: Vec<Event>,
    ) -> Result<(), RepositoryError> {
        let mut event_list = Vec::new();
        for event in events {
            event_list.push(entity::events::ActiveModel {
                id: Set(event.id.clone()),
                repository_id: Set(repository.into()),
                message: Set(event.message),
                unix_time_stamp: Set(event.unix_timestamp as i64),
                metadata: Set(Some(json!(event.metadata))),
            });
        }
        let _ = entity::events::Entity::insert_many(event_list)
            .on_conflict(
                OnConflict::column(entity::events::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_events(&self, repository: &str) -> Result<Vec<Event>, RepositoryError> {
        let events = entity::events::Entity::find()
            .filter(entity::events::Column::RepositoryId.eq(repository))
            .all(&self.conn)
            .await?;
        let mut event_list = Vec::new();
        for event in events {
            let metadata: HashMap<String, serde_json::Value> = event
                .metadata
                .map(|s| serde_json::from_value(s).unwrap())
                .unwrap_or_default();
            event_list.push(Event {
                id: event.id,
                message: event.message,
                unix_timestamp: event.unix_time_stamp as u64,
                metadata,
            });
        }
        Ok(event_list)
    }

    #[tracing::instrument]
    pub async fn add_content(
        &self,
        repository: &str,
        content_payloads: Vec<ContentPayload>,
    ) -> Result<(), RepositoryError> {
        let mut content_list = Vec::new();
        let mut extraction_events = Vec::new();
        for content_payload in content_payloads {
            info!("adding text: {}", &content_payload.id);
            content_list.push(entity::content::ActiveModel {
                id: Set(content_payload.id.clone()),
                repository_id: Set(repository.into()),
                payload: Set(content_payload.payload),
                payload_type: Set(content_payload.payload_type.to_string()),
                metadata: Set(Some(json!(content_payload.metadata))),
                content_type: Set(content_payload.content_type.to_string()),
                extractor_bindings_state: Set(Some(json!(ExtractorBindingsState::default()))),
            });
            let extraction_event = ExtractionEvent {
                id: nanoid!(),
                repository_id: repository.into(),
                payload: ExtractionEventPayload::CreateContent {
                    content_id: content_payload.id.clone(),
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

    #[tracing::instrument]
    pub async fn content_from_repo(
        &self,
        content_id: &str,
        repo_id: &str,
    ) -> Result<ContentPayload, RepositoryError> {
        let model = entity::content::Entity::find()
            .filter(entity::content::Column::RepositoryId.eq(repo_id))
            .filter(entity::content::Column::Id.eq(content_id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::ContentNotFound(content_id.to_owned()))?;
        Ok(ContentPayload {
            id: model.id,
            content_type: ContentType::from_str(&model.content_type).unwrap(),
            payload: model.payload,
            payload_type: PayloadType::from_str(&model.payload_type).unwrap(),
            metadata: serde_json::from_value(model.metadata.unwrap()).unwrap(),
        })
    }

    #[tracing::instrument]
    pub async fn content_with_unapplied_extractor(
        &self,
        repo_id: &str,
        extractor_binding: &ExtractorBinding,
        content_id: Option<&str>,
    ) -> Result<Vec<entity::content::Model>, RepositoryError> {
        let mut values = vec![repo_id.into(), extractor_binding.id.clone().into()];
        let mut query: String = "select * from content where repository_id=$1 and COALESCE(cast(extractor_bindings_state->'state'->>$2 as int),0) < 1".to_string();
        let mut idx = 3;
        if let Some(content_id) = content_id {
            values.push(content_id.into());
            query.push_str(format!(" and id = ${}", idx).as_str());
            idx += 1;
        }
        for filter in &extractor_binding.filters {
            match filter {
                ExtractorFilter::Eq { field, value } => {
                    values.push(field.to_string().into());
                    values.push(value.as_str().unwrap().into());
                    query.push_str(format!(" and metadata->>${} = ${}", idx, idx + 1).as_str());
                    idx += 2;
                }
                ExtractorFilter::Neq { field, value } => {
                    values.push(field.to_string().into());
                    values.push(value.as_str().unwrap().into());
                    query.push_str(format!(" and metadata->>${} != ${}", idx, idx + 1).as_str());
                    idx += 2;
                }
            }
        }
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

    #[tracing::instrument]
    pub async fn mark_content_as_processed(
        &self,
        content_id: &str,
        binding_id: &str,
    ) -> Result<(), anyhow::Error> {
        // TODO change the '1' to a timestamp so that the state value reflects
        // when was the worker state updated.
        let query = r#"update content set extractor_bindings_state['state'][$2] = '1' where id=$1"#;
        let values = vec![content_id.into(), binding_id.into()];
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

    #[tracing::instrument]
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

    #[tracing::instrument]
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

    #[tracing::instrument]
    pub async fn create_chunks(
        &self,
        chunks: Vec<Chunk>,
        index_name: &str,
    ) -> Result<(), RepositoryError> {
        let chunk_models: Vec<entity::chunked_content::ActiveModel> = chunks
            .iter()
            .map(|chunk| entity::chunked_content::ActiveModel {
                chunk_id: Set(chunk.chunk_id.clone()),
                content_id: Set(chunk.content_id.clone()),
                text: Set(chunk.text.clone()),
                index_name: Set(index_name.into()),
            })
            .collect();
        let result = entity::chunked_content::Entity::insert_many(chunk_models)
            .on_conflict(
                OnConflict::column(entity::chunked_content::Column::ChunkId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&self.conn)
            .await;
        if let Err(err) = result {
            if err != DbErr::RecordNotInserted {
                return Err(RepositoryError::DatabaseError(err));
            }
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn chunk_with_id(&self, id: &str) -> Result<ChunkWithMetadata, RepositoryError> {
        let chunk = entity::chunked_content::Entity::find()
            .filter(entity::chunked_content::Column::ChunkId.eq(id))
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

    #[tracing::instrument]
    pub async fn upsert_repository(
        &self,
        repository: DataRepository,
    ) -> Result<(), RepositoryError> {
        let mut extractor_event_models = Vec::new();
        let mut extractor_bindings = HashMap::new();
        for eb in &repository.extractor_bindings {
            extractor_bindings.insert(eb.id.clone(), eb.clone());
            let extractor_event = ExtractionEvent {
                id: nanoid!(),
                repository_id: repository.name.clone(),
                payload: ExtractionEventPayload::ExtractorBindingAdded {
                    repository: repository.name.clone(),
                    id: eb.id.clone(),
                },
            };
            let extraction_event_model = entity::extraction_event::ActiveModel {
                id: Set(extractor_event.id.clone()),
                payload: Set(json!(extractor_event)),
                allocation_info: NotSet,
                processed_at: NotSet,
            };
            extractor_event_models.push(extraction_event_model);
        }
        let repository_model = entity::data_repository::ActiveModel {
            name: Set(repository.name),
            extractor_bindings: Set(Some(json!(extractor_bindings))),
            metadata: Set(Some(json!(repository.metadata))),
            data_connectors: Set(Some(json!(repository.data_connectors))),
        };

        let _ = self
            .conn
            .transaction::<_, (), RepositoryError>(|txn| {
                Box::pin(async move {
                    let _ = DataRepositoryEntity::insert(repository_model)
                        .on_conflict(
                            OnConflict::column(entity::data_repository::Column::Name)
                                .update_columns(vec![
                                    entity::data_repository::Column::ExtractorBindings,
                                    entity::data_repository::Column::Metadata,
                                ])
                                .to_owned(),
                        )
                        .exec(txn)
                        .await?;
                    if !extractor_event_models.is_empty() {
                        // TODO Figure out why this doesn't throw an exception when the query fails
                        let _ = ExtractionEventEntity::insert_many(extractor_event_models)
                            .exec(txn)
                            .await?;
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|e| RepositoryError::LogicError(e.to_string()));

        Ok(())
    }

    #[tracing::instrument]
    pub async fn repositories(&self) -> Result<Vec<DataRepository>, RepositoryError> {
        let repository_models: Vec<DataRepository> = DataRepositoryEntity::find()
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect();
        Ok(repository_models)
    }

    #[tracing::instrument]
    pub async fn repository_by_name(&self, name: &str) -> Result<DataRepository, RepositoryError> {
        let repository_model = DataRepositoryEntity::find()
            .filter(entity::data_repository::Column::Name.eq(name))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(name.to_owned()))?;
        Ok(repository_model.into())
    }

    #[tracing::instrument]
    pub async fn extractor_by_name(
        &self,
        name: &str,
    ) -> Result<ExtractorDescription, RepositoryError> {
        let extractor_model = extractors::Entity::find()
            .filter(entity::extractors::Column::Id.eq(name))
            .one(&self.conn)
            .await;

        if let Err(e) = &extractor_model {
            error!("Error getting extractor by name {}: {:?}", name, e);
        }
        let extractor_model =
            extractor_model?.ok_or(RepositoryError::ExtractorNotFound(name.to_owned()))?;
        Ok(extractor_model.into())
    }

    #[tracing::instrument]
    pub async fn add_attributes(
        &self,
        repository: &str,
        index_name: &str,
        extracted_attributes: ExtractedAttributes,
    ) -> Result<(), RepositoryError> {
        let attribute_index_model = entity::attributes_index::ActiveModel {
            id: Set(extracted_attributes.id.clone()),
            repository_id: Set(repository.into()),
            index_name: Set(index_name.into()),
            extractor_id: Set(extracted_attributes.extractor_name),
            data: Set(extracted_attributes.attributes.clone()),
            content_id: Set(extracted_attributes.content_id.clone()),
            created_at: Set(0),
        };
        entity::attributes_index::Entity::insert(attribute_index_model)
            .on_conflict(
                OnConflict::column(entity::attributes_index::Column::Id)
                    .update_columns(vec![
                        entity::attributes_index::Column::Data,
                        entity::attributes_index::Column::CreatedAt,
                    ])
                    .to_owned(),
            )
            .exec(&self.conn)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get_extracted_attributes(
        &self,
        repository: &str,
        index: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedAttributes>, RepositoryError> {
        let query = entity::attributes_index::Entity::find()
            .filter(entity::attributes_index::Column::RepositoryId.eq(repository))
            .filter(entity::attributes_index::Column::IndexName.eq(index))
            .apply_if(content_id, |query, v| {
                query.filter(entity::attributes_index::Column::ContentId.eq(v))
            });

        let extracted_attributes: Vec<ExtractedAttributes> = query
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|v| v.into())
            .collect::<Vec<ExtractedAttributes>>();
        Ok(extracted_attributes)
    }

    #[tracing::instrument]
    pub async fn record_extractors(
        &self,
        extractors: Vec<ExtractorDescription>,
    ) -> Result<(), RepositoryError> {
        let mut extractor_models: Vec<entity::extractors::ActiveModel> = vec![];
        for extractor in extractors {
            extractor_models.push(entity::extractors::ActiveModel {
                id: Set(extractor.name),
                description: Set(extractor.description),
                input_params: Set(extractor.input_params),
                output_schema: Set(json!(extractor.schemas)),
            });
        }
        let res = entity::extractors::Entity::insert_many(extractor_models)
            .on_conflict(
                OnConflict::column(entity::extractors::Column::Id)
                    .update_columns(vec![
                        entity::extractors::Column::Description,
                        entity::extractors::Column::InputParams,
                    ])
                    .to_owned(),
            )
            .exec(&self.conn)
            .await;
        if let Err(err) = res {
            if err != DbErr::RecordNotInserted {
                return Err(RepositoryError::DatabaseError(err));
            }
        }

        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_extractors(&self) -> Result<Vec<ExtractorDescription>, RepositoryError> {
        let extractor_models: Vec<ExtractorDescription> = extractors::Entity::find()
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect();
        Ok(extractor_models)
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_extractor(
        &self,
        extractor_name: &str,
    ) -> Result<ExtractorDescription, RepositoryError> {
        let extractor_config = extractors::Entity::find()
            .filter(entity::extractors::Column::Id.eq(extractor_name))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(
                extractor_name.to_owned(),
            ))?;
        Ok(extractor_config.into())
    }

    #[tracing::instrument(skip(self))]
    pub async fn insert_work(&self, work: &Work) -> Result<(), RepositoryError> {
        let work_model = entity::work::ActiveModel {
            id: Set(work.id.clone()),
            state: Set(work.work_state.to_string()),
            worker_id: Set(work.executor_id.as_ref().map(|id| id.to_owned())),
            content_id: Set(work.content_id.clone()),
            index_names: Set(serde_json::to_value(work.indexes.clone()).unwrap()),
            extractor: Set(work.extractor.clone()),
            extractor_params: Set(work.extractor_params.clone()),
            repository_id: Set(work.repository_id.clone()),
        };
        WorkEntity::insert(work_model).exec(&self.conn).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn work_by_id(&self, id: &str) -> Result<Work, RepositoryError> {
        let work_model = WorkEntity::find()
            .filter(entity::work::Column::Id.eq(id))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(id.into()))?;
        Ok(work_model.try_into().unwrap())
    }

    #[tracing::instrument(skip(self))]
    pub async fn unallocated_work(&self) -> Result<Vec<work::Model>, RepositoryError> {
        let work_models = WorkEntity::find()
            .filter(entity::work::Column::WorkerId.is_null())
            .filter(entity::work::Column::State.eq(WorkState::Pending.to_string()))
            .all(&self.conn)
            .await?;
        Ok(work_models)
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    pub async fn update_work_state(
        &self,
        work_id: &str,
        state: &WorkState,
    ) -> Result<Work, RepositoryError> {
        let result = entity::work::Entity::update_many()
            .col_expr(entity::work::Column::State, Expr::value(state.to_string()))
            .filter(entity::work::Column::Id.eq(work_id))
            .exec_with_returning(&self.conn)
            .await?;
        if result.is_empty() {
            return Err(RepositoryError::LogicError(
                "unable to find work".to_string(),
            ));
        }
        result
            .get(0)
            .map(|r| r.to_owned().try_into().unwrap())
            .ok_or(RepositoryError::LogicError(
                "unable to find work".to_string(),
            ))
    }

    #[tracing::instrument(skip(self))]
    pub async fn work_for_worker(&self, worker_id: &str) -> Result<Vec<Work>, RepositoryError> {
        let work_models = WorkEntity::find()
            .filter(entity::work::Column::WorkerId.eq(worker_id))
            .filter(entity::work::Column::State.eq(WorkState::Pending.to_string()))
            .all(&self.conn)
            .await?
            .into_iter()
            .map(|m| m.try_into().unwrap())
            .collect();
        Ok(work_models)
    }

    #[tracing::instrument(skip(self))]
    pub async fn binding_by_id(
        &self,
        repository: &str,
        id: &str,
    ) -> Result<ExtractorBinding, RepositoryError> {
        let query = "select name, metadata, data_connectors, extractor_bindings  from data_repository, jsonb_each(data_repository.extractor_bindings) binding_ids where binding_ids.key = $1";
        let data_repository = entity::data_repository::Entity::find()
            .from_raw_sql(Statement::from_sql_and_values(
                DbBackend::Postgres,
                query,
                vec![id.into()],
            ))
            .one(&self.conn)
            .await?
            .ok_or(RepositoryError::RepositoryNotFound(repository.into()))?;

        let bindings_map: HashMap<String, ExtractorBinding> =
            serde_json::from_value(data_repository.extractor_bindings.unwrap()).unwrap();
        Ok(bindings_map.get(id).unwrap().clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::db_utils::create_db;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_extractors_for_repository() {
        let extractor1 = ExtractorDescription {
            name: "extractor1".into(),
            description: "extractor1".into(),
            input_params: json!({}),
            schemas: ExtractorSchema::from_output_schema(
                "embedding",
                ExtractorOutputSchema::embedding(10, IndexDistance::Cosine),
            ),
        };
        let extractor_binding1 = ExtractorBinding::new(
            "repository",
            "extractor1".into(),
            IndexBindings::from_feature("extractor1"),
            vec![ExtractorFilter::Eq {
                field: "topic".to_string(),
                value: json!("pipe"),
            }],
            serde_json::json!({}),
        );

        let extractor_binding2 = ExtractorBinding::new(
            "repository1",
            "extractor1".into(),
            IndexBindings::from_feature("extractor1_output"),
            vec![ExtractorFilter::Neq {
                field: "topic".to_string(),
                value: json!("pipe"),
            }],
            serde_json::json!({}),
        );
        let repo = DataRepository {
            name: "test".to_owned(),
            data_connectors: vec![],
            extractor_bindings: vec![extractor_binding1.clone()],
            metadata: HashMap::new(),
        };

        let db = create_db().await.unwrap();
        let repository = Repository::new_with_db(db);
        repository
            .record_extractors(vec![extractor1])
            .await
            .unwrap();
        repository.upsert_repository(repo.clone()).await.unwrap();

        repository
            .add_content(
                &repo.name,
                vec![
                    ContentPayload::from_text(
                        "test",
                        "hello",
                        HashMap::from([("topic".to_string(), json!("pipe"))]),
                    ),
                    ContentPayload::from_text(
                        "test",
                        "world",
                        HashMap::from([("topic".to_string(), json!("baz"))]),
                    ),
                ],
            )
            .await
            .unwrap();

        let content_list1 = repository
            .content_with_unapplied_extractor(&repo.name, &extractor_binding1, None)
            .await
            .unwrap();
        assert_eq!(1, content_list1.len());

        let content_list2 = repository
            .content_with_unapplied_extractor(&repo.name, &extractor_binding2, None)
            .await
            .unwrap();
        assert_eq!(1, content_list2.len());
        assert_ne!(content_list1[0].id, content_list2[0].id);
    }
}
