use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use entity::{
    data_repository::Entity as DataRepositoryEntity,
    extraction_event::Entity as ExtractionEventEntity,
    extractors,
    index::{Entity as IndexEntity, Model as IndexModel},
    work::Entity as WorkEntity,
};
use mime::Mime;
use nanoid::nanoid;
use sea_orm::{
    sea_query::{Expr, OnConflict},
    ActiveModelTrait,
    ActiveValue::NotSet,
    ColumnTrait,
    ConnectOptions,
    ConnectionTrait,
    Database,
    DatabaseConnection,
    DbBackend,
    DbErr,
    EntityTrait,
    QueryFilter,
    QueryTrait,
    Set,
    Statement,
    TransactionTrait,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use smart_default::SmartDefault;
use strum::{Display, EnumString};
use thiserror::Error;
use tracing::{error, info};

use crate::{
    entity,
    entity::{index, work},
    vectordbs::{self, IndexDistance},
};

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
    pub content_type: mime::Mime,
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
            content_type: mime::TEXT_PLAIN,
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
        let mime_type = mime_guess::from_path(name).first_or_octet_stream();
        Self {
            id,
            content_type: mime_type,
            payload: path.into(),
            payload_type: PayloadType::BlobStorageLink,
            metadata: HashMap::new(),
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
    pub extractor_binding: String,
    pub extractor_params: serde_json::Value,
    pub work_state: WorkState,
    pub executor_id: Option<String>,
}

impl Work {
    pub fn new(
        content_id: &str,
        repository: &str,
        extractor: &str,
        extractor_binding: &str,
        extractor_params: &serde_json::Value,
        worker_id: Option<&str>,
    ) -> Self {
        let mut s = DefaultHasher::new();
        content_id.hash(&mut s);
        repository.hash(&mut s);
        extractor.hash(&mut s);
        extractor_binding.hash(&mut s);
        let id = format!("{:x}", s.finish());

        Self {
            id,
            content_id: content_id.into(),
            repository_id: repository.into(),
            extractor: extractor.into(),
            extractor_binding: extractor_binding.into(),
            extractor_params: extractor_params.clone(),
            work_state: WorkState::Pending,
            executor_id: worker_id.map(|w| w.into()),
        }
    }
}

impl TryFrom<work::Model> for Work {
    type Error = anyhow::Error;

    fn try_from(model: work::Model) -> Result<Self, anyhow::Error> {
        Ok(Self {
            id: model.id,
            content_id: model.content_id,
            repository_id: model.repository_id,
            extractor: model.extractor,
            extractor_binding: model.extractor_binding,
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

    #[error("content`{0}` not found")]
    ContentNotFound(String),
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
    pub async fn chunk_with_id(&self, id: &str) -> Result<ChunkWithMetadata> {
        let chunk = entity::chunked_content::Entity::find()
            .filter(entity::chunked_content::Column::ChunkId.eq(id))
            .one(&self.conn)
            .await?
            .ok_or(anyhow!("chunk id: {} not found", id))?;
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
}
