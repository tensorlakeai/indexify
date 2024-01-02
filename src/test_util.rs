#[cfg(test)]
pub mod db_utils {
    use std::{collections::HashMap, sync::Arc};

    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DatabaseConnection, DbErr};
    use serde_json::json;
    use utoipa_swagger_ui::serve;

    use crate::{
        attribute_index::AttributeIndexManager,
        coordinator::Coordinator,
        coordinator_service::CoordinatorServer,
        executor::ExtractorExecutor,
        extractor::{extractor_runner, py_extractors},
        internal_api::ExtractorHeartbeat,
        persistence::{
            DataRepository,
            Extractor,
            ExtractorBinding,
            ExtractorOutputSchema,
            ExtractorSchema,
            Repository,
        },
        server_config::{ExtractorConfig, ServerConfig},
        state,
        vector_index::VectorIndexManager,
        vectordbs::{self, qdrant::QdrantDb, IndexDistance, VectorDBTS},
    };

    pub const DEFAULT_TEST_REPOSITORY: &str = "test_repository";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MockExtractor";

    pub fn default_test_data_repository() -> DataRepository {
        DataRepository {
            name: DEFAULT_TEST_REPOSITORY.into(),
            data_connectors: vec![],
            metadata: HashMap::new(),
            extractor_bindings: vec![ExtractorBinding::new(
                "test_extractor_binding",
                DEFAULT_TEST_REPOSITORY,
                DEFAULT_TEST_EXTRACTOR.into(),
                vec![],
                serde_json::json!({}),
            )],
        }
    }

    fn mock_extractor_config() -> ExtractorConfig {
        ExtractorConfig {
            name: DEFAULT_TEST_EXTRACTOR.into(),
            version: "0.1.0".into(),
            description: "test extractor".into(),
            module: "indexify_extractor_sdk.mock_extractor:MockExtractor".into(),
            gpu: false,
            system_dependencies: vec![],
            python_dependencies: vec![],
        }
    }

    pub async fn create_index_manager(
        db: DatabaseConnection,
    ) -> (ExtractorExecutor, CoordinatorServer) {
        let index_name = format!("{}/{}", DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR);
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::server_config::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        let _ = qdrant.drop_index(index_name).await;
        let repository = Arc::new(Repository::new_with_db(db.clone()));
        let server_config = Arc::new(ServerConfig::from_path("local_server_config.yaml").unwrap());
        let executor_config = Arc::new(crate::server_config::ExecutorConfig::default());
        let extractor_config = Arc::new(mock_extractor_config());
        let extractor =
            py_extractors::PythonExtractor::new_from_extractor_path(&extractor_config.module)
                .unwrap();
        let extractor_runner =
            extractor_runner::ExtractorRunner::new(Arc::new(extractor), mock_extractor_config());
        let extractor_executor =
            ExtractorExecutor::new_test(repository.clone(), executor_config, extractor_runner)
                .unwrap();
        let coordinator_svr = CoordinatorServer::new(server_config).await.unwrap();
        (extractor_executor, coordinator_svr)
    }

    pub async fn create_db() -> Result<DatabaseConnection, DbErr> {
        let db = Database::connect("postgres://postgres:postgres@localhost/indexify_test").await?;
        Migrator::fresh(&db).await?;

        Ok(db)
    }
}
