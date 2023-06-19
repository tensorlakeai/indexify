#[cfg(test)]
pub mod db_utils {
    use migration::{Migrator, MigratorTrait};
    use std::sync::Arc;

    use sea_orm::{Database, DatabaseConnection, DbErr};

    use crate::extractors::ExtractorRunner;
    use crate::persistence::Repository;
    use crate::{
        index::IndexManager, vectordbs::qdrant::QdrantDb, EmbeddingRouter, QdrantConfig, ServerConfig,
        vectordbs::VectorDBTS, VectorIndexConfig,
    };

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
        let db = Database::connect("postgres://postgres:postgres@localhost/indexify_test").await?;
        Migrator::fresh(&db).await?;

        Ok(db)
    }
}
