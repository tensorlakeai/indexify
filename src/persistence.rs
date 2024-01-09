use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use anyhow::Result;
use sea_orm::{
    sea_query::OnConflict,
    ColumnTrait,
    ConnectOptions,
    Database,
    DatabaseConnection,
    EntityTrait,
    QueryFilter,
    QueryTrait,
    Set,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::entity;
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

#[derive(Debug)]
pub struct Repository {
    conn: DatabaseConnection,
}

impl Repository {
    pub async fn new(db_url: &str) -> Result<Self> {
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
    pub async fn add_attributes(
        &self,
        repository: &str,
        index_name: &str,
        extracted_attributes: ExtractedAttributes,
    ) -> Result<()> {
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
    ) -> Result<Vec<ExtractedAttributes>> {
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
