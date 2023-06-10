use sea_orm::DbConn;
use std::sync::Arc;
use thiserror::Error;

use crate::persistence::{DataRepository, Repository, RepositoryError};

#[derive(Error, Debug)]
pub enum DataRepositoryError {
    #[error(transparent)]
    Persistence(#[from] RepositoryError),
}

pub struct DataRepositoryManager {
    repository: Arc<Repository>,
}

impl DataRepositoryManager {
    pub async fn new(db_url: &str) -> Result<Self, RepositoryError> {
        let repository = Arc::new(Repository::new(db_url).await?);
        Ok(Self { repository })
    }

    #[allow(dead_code)]
    fn new_with_db(db: DbConn) -> Self {
        let repository = Arc::new(Repository::new_with_db(db));
        Self { repository }
    }

    pub async fn list_repositories(&self) -> Result<Vec<DataRepository>, DataRepositoryError> {
        self.repository
            .repositories()
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn sync(&self, repository: &DataRepository) -> Result<(), DataRepositoryError> {
        self.repository
            .upsert_repository(repository.to_owned())
            .await
            .map_err(DataRepositoryError::Persistence)
    }

    pub async fn get(&self, name: String) -> Result<DataRepository, DataRepositoryError> {
        self.repository
            .repository_by_name(name.clone())
            .await
            .map_err(DataRepositoryError::Persistence)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use crate::persistence::{Extractor, ExtractorType, IndexDistance};
    use crate::text_splitters::TextSplitterKind;
    use sea_orm::entity::prelude::*;
    use sea_orm::{
        sea_query::TableCreateStatement, Database, DatabaseConnection, DbBackend, DbConn, DbErr,
        Schema,
    };
    use serde_json::json;

    use super::super::entity::data_repository::Entity as DataRepositoryEntity;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_repository() {
        let db = create_db().await.unwrap();
        let repository_manager = DataRepositoryManager::new_with_db(db);
        let mut meta = HashMap::new();
        meta.insert("foo".to_string(), json!(12));
        let repository = DataRepository {
            name: "test".to_string(),
            extractors: vec![Extractor {
                name: "test".to_string(),
                extractor_type: ExtractorType::Embedding {
                    model: "m1".into(),
                    text_splitter: TextSplitterKind::Noop,
                    distance: IndexDistance::Cosine,
                },
            }],
            metadata: meta.clone(),
        };
        repository_manager.sync(&repository).await.unwrap();
        let repositories = repository_manager.list_repositories().await.unwrap();
        assert_eq!(repositories.len(), 1);
        assert_eq!(repositories[0].name, "test");
        assert_eq!(repositories[0].extractors.len(), 1);
        assert_eq!(repositories[0].metadata, meta);
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
        let stmt1: TableCreateStatement = schema.create_table_from_entity(DataRepositoryEntity);

        // Execute create table statement
        db.execute(db.get_database_backend().build(&stmt1)).await?;
        Ok(())
    }
}
