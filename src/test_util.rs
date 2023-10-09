#[cfg(test)]
pub mod db_utils {
    use migration::{Migrator, MigratorTrait};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    use sea_orm::{Database, DatabaseConnection, DbErr};

    use crate::attribute_index::AttributeIndexManager;
    use crate::executor::ExtractorExecutor;
    use crate::persistence::{DataRepository, ExtractorOutputSchema};
    use crate::persistence::{ExtractorBinding, ExtractorConfig, Repository};
    use crate::vector_index::VectorIndexManager;
    use crate::vectordbs::{self, IndexDistance};
    use crate::Coordinator;
    use crate::{vectordbs::qdrant::QdrantDb, vectordbs::VectorDBTS, ServerConfig};

    pub const DEFAULT_TEST_REPOSITORY: &str = "test_repository";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MiniLML6";

    pub fn default_test_data_repository() -> DataRepository {
        DataRepository {
            name: DEFAULT_TEST_REPOSITORY.into(),
            data_connectors: vec![],
            metadata: HashMap::new(),
            extractor_bindings: vec![ExtractorBinding::new(
                DEFAULT_TEST_REPOSITORY,
                DEFAULT_TEST_EXTRACTOR.into(),
                DEFAULT_TEST_EXTRACTOR.into(),
                vec![],
                serde_json::json!({}),
            )],
        }
    }

    pub async fn create_index_manager(
        db: DatabaseConnection,
    ) -> (Arc<VectorIndexManager>, ExtractorExecutor, Arc<Coordinator>) {
        let index_name = format!("{}/{}", DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR);
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        let _ = qdrant.drop_index(index_name).await;
        let repository = Arc::new(Repository::new_with_db(db.clone()));
        let coordinator = Coordinator::new(repository.clone());
        let server_config = Arc::new(ServerConfig::from_path("local_server_config.yaml").unwrap());
        let executor_config =
            Arc::new(crate::ExecutorConfig::from_path("local_executor_config.yaml").unwrap());
        let vector_db =
            vectordbs::create_vectordb(server_config.index_config.clone(), db.clone()).unwrap();
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            repository.clone(),
            vector_db,
            "localhost:9000".to_string(),
        ));
        let attribute_index_manage = Arc::new(AttributeIndexManager::new(repository.clone()));
        let extractor_executor = ExtractorExecutor::new_test(
            repository.clone(),
            executor_config,
            vector_index_manager.clone(),
            attribute_index_manage.clone(),
        )
        .unwrap();
        coordinator
            .record_executor(extractor_executor.get_executor_info())
            .await
            .unwrap();

        let default_extractor = ExtractorConfig {
            name: DEFAULT_TEST_EXTRACTOR.into(),
            description: "test extractor".into(),
            input_params: json!({}),
            output_schema: ExtractorOutputSchema::Embedding {
                dim: 384,
                distance: IndexDistance::Cosine,
            },
        };
        coordinator
            .record_extractor(default_extractor)
            .await
            .unwrap();
        (vector_index_manager, extractor_executor, coordinator)
    }

    pub async fn create_db() -> Result<DatabaseConnection, DbErr> {
        let db = Database::connect("postgres://postgres:postgres@localhost/indexify_test").await?;
        Migrator::fresh(&db).await?;

        Ok(db)
    }
}
