#[cfg(test)]
pub mod db_utils {
    use migration::{Migrator, MigratorTrait};
    use std::collections::HashMap;
    use std::sync::Arc;

    use sea_orm::{Database, DatabaseConnection, DbErr};

    use crate::attribute_index::AttributeIndexManager;
    use crate::executor::ExtractorExecutor;
    use crate::persistence::DataRepository;
    use crate::persistence::{ExtractorBinding, ExtractorConfig, ExtractorType, Repository};
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
            extractor_bindings: vec![ExtractorBinding {
                extractor_name: DEFAULT_TEST_EXTRACTOR.into(),
                index_name: DEFAULT_TEST_EXTRACTOR.into(),
                filters: vec![],
                input_params: serde_json::json!({}),
            }],
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
        let repo = Arc::new(Repository::new_with_db(db.clone()));
        let coordinator = Coordinator::new(repo.clone());
        let server_config = Arc::new(ServerConfig::from_path("local_config.yaml").unwrap());
        let vector_db = vectordbs::create_vectordb(server_config.index_config.clone()).unwrap();
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            server_config.clone(),
            repo.clone(),
            vector_db,
        ));
        let attribute_index_manage = Arc::new(AttributeIndexManager::new(repo.clone()));
        let extractor_executor = ExtractorExecutor::new_test(
            repo.clone(),
            server_config.clone(),
            vector_index_manager.clone(),
            attribute_index_manage.clone(),
        )
        .unwrap();
        coordinator
            .record_node(extractor_executor.get_executor_info())
            .await
            .unwrap();

        let default_extractor = ExtractorConfig {
            name: DEFAULT_TEST_EXTRACTOR.into(),
            extractor_type: ExtractorType::Embedding {
                dim: 384,
                distance: IndexDistance::Cosine,
            },
            ..Default::default()
        };
        coordinator
            .record_extractors(vec![default_extractor])
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
