#[cfg(test)]
pub mod db_utils {
    use migration::{Migrator, MigratorTrait};
    use std::collections::HashMap;
    use std::sync::Arc;

    use sea_orm::{Database, DatabaseConnection, DbErr};

    use crate::executor::ExtractorExecutor;
    use crate::persistence::DataRepository;
    use crate::persistence::{self, ExtractorBinding, ExtractorConfig, ExtractorType, Repository};
    use crate::vectordbs::IndexDistance;
    use crate::Coordinator;
    use crate::{
        index::IndexManager, vectordbs::qdrant::QdrantDb, vectordbs::VectorDBTS, EmbeddingRouter,
        QdrantConfig, ServerConfig, VectorIndexConfig,
    };

    pub const DEFAULT_TEST_REPOSITORY: &str = "test_repository";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "test_extractor";

    pub fn default_test_data_repository() -> DataRepository {
        DataRepository {
            name: DEFAULT_TEST_REPOSITORY.into(),
            data_connectors: vec![],
            metadata: HashMap::new(),
            extractor_bindings: vec![ExtractorBinding {
                name: DEFAULT_TEST_EXTRACTOR.into(),
                index_name: DEFAULT_TEST_EXTRACTOR.into(),
                text_splitter: crate::text_splitters::TextSplitterKind::NewLine,
                filter: persistence::ExtractorFilter::ContentType {
                    content_type: persistence::ContentType::Text,
                },
            }],
        }
    }

    pub async fn create_index_manager(
        db: DatabaseConnection,
    ) -> (Arc<IndexManager>, ExtractorExecutor, Arc<Coordinator>) {
        let index_name = format!("{}/{}", DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR);
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        let _ = qdrant.drop_index(index_name).await;
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
        let coordinator = Coordinator::new(repo.clone());
        let extractor_executor = ExtractorExecutor::new_test(
            repo,
            index_manager.clone(),
            Arc::new(ServerConfig::default()),
        );
        coordinator
            .record_node(extractor_executor.get_executor_info())
            .await
            .unwrap();

        let default_extractor = ExtractorConfig {
            name: DEFAULT_TEST_EXTRACTOR.into(),
            extractor_type: ExtractorType::Embedding {
                model: ServerConfig::default()
                    .default_model()
                    .model_kind
                    .to_string(),
                distance: IndexDistance::Cosine,
            },
        };
        coordinator
            .record_extractors(vec![default_extractor])
            .await
            .unwrap();
        (index_manager, extractor_executor, coordinator)
    }

    pub async fn create_db() -> Result<DatabaseConnection, DbErr> {
        let db = Database::connect("postgres://postgres:postgres@localhost/indexify_test").await?;
        Migrator::fresh(&db).await?;

        Ok(db)
    }
}
