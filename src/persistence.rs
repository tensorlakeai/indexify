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

#[derive(Debug, Error)]
pub enum RespositoryError {
    #[error(transparent)]
    DatabaseError(#[from] DbErr),

    #[error("index `{0}` not found")]
    IndexNotFound(String),
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
        index: String,
        embedding_model: String,
        vector_db: String,
        text_splitter: String,
    ) -> Result<(), RespositoryError> {
        let index = entity::index::ActiveModel {
            name: Set(index),
            embedding_model: Set(embedding_model),
            text_splitter: Set(text_splitter),
            vector_db: Set(vector_db),
            vector_db_params: NotSet,
        };
        let tx = self.conn.begin().await?;
        let _result = IndexEntity::insert(index).exec(&tx).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_index(&self, index: String) -> Result<IndexModel, RespositoryError> {
        let result = IndexEntity::find()
            .filter(index::Column::Name.eq(&index))
            .one(&self.conn)
            .await?
            .ok_or(RespositoryError::IndexNotFound(index));
        result
    }
}
