use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use entity::index::Entity as IndexEntity;
use entity::index::Model as IndexModel;
use sea_orm::ColumnTrait;
use sea_orm::QueryFilter;
use sea_orm::{
    ActiveValue::NotSet, Database, DatabaseConnection, DbErr, EntityTrait, Set, TransactionTrait,
};
use thiserror::Error;

use crate::entity;
use crate::entity::index;
use crate::vectordbs::{self, CreateIndexParams};
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct Text {
    pub text: String,
    pub metadata: HashMap<String, String>,
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
pub enum RespositoryError {
    #[error(transparent)]
    DatabaseError(#[from] DbErr),

    #[error(transparent)]
    VectorDb(#[from] vectordbs::VectorDbError),

    #[error("index `{0}` not found")]
    IndexNotFound(String),

    #[error("content`{0}` not found")]
    ContentNotFound(String),

    #[error("index `{0}` already exists")]
    IndexAlreadyExists(String),

    #[error("unable to serialize unique params `{0}`")]
    UniqueParamsSerializationError(#[from] serde_json::Error),
}

pub struct Respository {
    conn: DatabaseConnection,
}

impl Respository {
    pub async fn new(db_url: &str) -> Result<Self, RespositoryError> {
        let db = Database::connect(db_url).await?;
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
    ) -> Result<(), RespositoryError> {
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
        };
        let tx = self.conn.begin().await?;
        let insert_result = IndexEntity::insert(index).exec(&tx).await;
        if let Err(db_err) = insert_result {
            // TODO Remvoe this hack and drop down to the underlying sqlx error
            // and check if the error is due to primary key violation
            if db_err.to_string().contains("code: 1555") {
                tx.rollback().await?;
                return Err(RespositoryError::IndexAlreadyExists(index_params.name));
            }
        }
        if let Err(err) = vectordb.create_index(index_params.clone()).await {
            tx.rollback().await?;
            return Err(RespositoryError::VectorDb(err));
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_index(&self, index: String) -> Result<IndexModel, RespositoryError> {
        IndexEntity::find()
            .filter(index::Column::Name.eq(&index))
            .one(&self.conn)
            .await?
            .ok_or(RespositoryError::IndexNotFound(index))
    }

    pub async fn get_texts(&self, index_name: String) -> Result<Vec<Text>, RespositoryError> {
        let index = self.get_index(index_name).await?;
        let contents = entity::content::Entity::find()
            .filter(entity::content::Column::IndexName.eq(&index.name))
            .all(&self.conn)
            .await?;
        let mut texts = Vec::new();
        for content in contents {
            let metadata: HashMap<String, String> = serde_json::from_str(
                content
                    .metadata
                    .as_ref()
                    .ok_or(RespositoryError::ContentNotFound(content.id))?,
            )?;
            texts.push(Text {
                text: content.text,
                metadata,
            });
        }
        Ok(texts)
    }

    pub async fn add_to_index(
        &self,
        index_name: String,
        texts: Vec<Text>,
    ) -> Result<(), RespositoryError> {
        let tx = self.conn.begin().await?;
        let mut content_list = Vec::new();
        for text in texts {
            let meta = serde_json::to_string(&text.metadata)?;
            let content_id = create_content_id(&index_name, &text.text);
            content_list.push(entity::content::ActiveModel {
                id: Set(content_id),
                index_name: Set(index_name.clone()),
                text: Set(text.text),
                metadata: Set(Some(meta)),
                embedding_status: NotSet,
                content_type: Set("document".to_string()),
            });
        }
        entity::content::Entity::insert_many(content_list)
            .exec(&tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_chunks(
        &self,
        content_id: String,
        chunks: Vec<Chunk>,
        index_name: String,
    ) -> Result<(), RespositoryError> {
        let tx = self.conn.begin().await?;
        let chunk_models: Vec<entity::index_chunks::ActiveModel> = chunks
            .iter()
            .map(|chunk| entity::index_chunks::ActiveModel {
                chunk_id: Set(chunk.chunk_id.clone()),
                content_id: Set(content_id.clone()),
                text: Set(chunk.text.clone()),
                index_name: Set(index_name.clone()),
            })
            .collect();
        entity::index_chunks::Entity::insert_many(chunk_models)
            .exec(&tx)
            .await?;

        // Mark the content as indexed
        let content_entity = entity::content::Entity::find()
            .filter(entity::content::Column::Id.eq(&content_id))
            .one(&tx)
            .await?
            .ok_or(RespositoryError::ContentNotFound(content_id.to_string()))?;
        let now = OffsetDateTime::now_utc();
        let mut content_entity: entity::content::ActiveModel = content_entity.into();
        content_entity.embedding_status = Set(Some(now.to_string()));
        tx.commit().await?;
        Ok(())
    }

    pub async fn not_indexed_content(
        &self,
    ) -> Result<Vec<entity::content::Model>, RespositoryError> {
        let result = entity::content::Entity::find()
            .filter(entity::content::Column::EmbeddingStatus.is_null())
            .all(&self.conn)
            .await?;
        Ok(result)
    }
}

fn create_content_id(index_name: &str, text: &str) -> String {
    let mut s = DefaultHasher::new();
    index_name.hash(&mut s);
    text.hash(&mut s);
    format!("{:x}", s.finish())
}
