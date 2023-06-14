#[cfg(test)]
pub mod db_utils {
    use sea_orm::entity::prelude::*;
    use std::sync::Arc;

    use sea_orm::{
        sea_query::TableCreateStatement, Database, DatabaseConnection, DbBackend, DbConn, DbErr,
        Schema,
    };

    use crate::extractors::ExtractorRunner;
    use crate::persistence::Repository;
    use crate::{
        index::IndexManager, qdrant::QdrantDb, EmbeddingRouter, QdrantConfig, ServerConfig,
        VectorDBTS, VectorIndexConfig,
    };

    use super::super::entity::content::Entity as ContentEntity;
    use super::super::entity::data_repository::Entity as DataRepositoryEntity;
    use super::super::entity::index::Entity as IndexEntity;
    use super::super::entity::index_chunks::Entity as IndexChunkEntity;
    use super::super::entity::memory_sessions::Entity as MemorySessionEntity;

    pub async fn create_repository() -> Arc<Repository> {
        let db = create_db().await.unwrap();
        Arc::new(Repository::new_with_db(db.clone()))
    }

    pub async fn create_index_manager(
        db: DatabaseConnection,
        index_name: &str,
    ) -> (Arc<IndexManager>, ExtractorRunner) {
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        let _ = qdrant.drop_index(index_name.into()).await;
        let embedding_router =
            Arc::new(EmbeddingRouter::new(Arc::new(ServerConfig::default())).unwrap());
        let index_config = VectorIndexConfig {
            index_store: crate::IndexStoreKind::Qdrant,
            qdrant_config: Some(QdrantConfig {
                addr: "http://localhost:6334".into(),
            }),
        };
        let index_manager = Arc::new(
            IndexManager::new_with_db(index_config, embedding_router, db.clone()).unwrap(),
        );
        let repo = Arc::new(Repository::new_with_db(db.clone()));
        let extractor_runner = ExtractorRunner::new(repo, index_manager.clone());
        (index_manager, extractor_runner)
    }

    pub async fn create_db() -> Result<DatabaseConnection, DbErr> {
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
        let stmt4: TableCreateStatement = schema.create_table_from_entity(DataRepositoryEntity);
        let stmt5: TableCreateStatement = schema.create_table_from_entity(MemorySessionEntity);

        // Execute create table statement
        db.execute(db.get_database_backend().build(&stmt1)).await?;
        db.execute(db.get_database_backend().build(&stmt2)).await?;
        db.execute(db.get_database_backend().build(&stmt3)).await?;
        db.execute(db.get_database_backend().build(&stmt4)).await?;
        db.execute(db.get_database_backend().build(&stmt5)).await?;
        Ok(())
    }
}
