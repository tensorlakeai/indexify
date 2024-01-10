#[cfg(test)]
pub mod db_utils {
    use std::{collections::HashMap, sync::Arc};

    use serde_json::json;

    use crate::{
        coordinator_service::CoordinatorServer,
        executor::ExtractorExecutor,
        extractor::{extractor_runner, py_extractors},
        internal_api::{EmbeddingSchema, ExtractorDescription, OutputSchema},
        server_config::{ExtractorConfig, ServerConfig},
        vectordbs::{qdrant::QdrantDb, VectorDBTS},
    };

    pub const DEFAULT_TEST_REPOSITORY: &str = "test_repository";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MockExtractor";

    pub fn mock_extractor() -> ExtractorDescription {
        let mut outputs = HashMap::new();
        outputs.insert(
            "test_output".to_string(),
            OutputSchema::Embedding(EmbeddingSchema {
                dim: 384,
                distance: "cosine".to_string(),
            }),
        );
        ExtractorDescription {
            name: DEFAULT_TEST_EXTRACTOR.to_string(),
            description: "test_desription".to_string(),
            input_params: json!({}),
            outputs,
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

    pub async fn create_index_manager() -> (ExtractorExecutor, CoordinatorServer) {
        let index_name = format!("{}/{}", DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR);
        let qdrant: VectorDBTS = Arc::new(QdrantDb::new(crate::server_config::QdrantConfig {
            addr: "http://localhost:6334".into(),
        }));
        let _ = qdrant.drop_index(index_name).await;
        let server_config = Arc::new(ServerConfig::from_path("local_server_config.yaml").unwrap());
        let executor_config = Arc::new(crate::server_config::ExecutorConfig::default());
        let extractor_config = Arc::new(mock_extractor_config());
        let extractor =
            py_extractors::PythonExtractor::new_from_extractor_path(&extractor_config.module)
                .unwrap();
        let extractor_runner =
            extractor_runner::ExtractorRunner::new(Arc::new(extractor), mock_extractor_config());
        let extractor_executor =
            ExtractorExecutor::new_test(executor_config, extractor_runner).unwrap();
        let coordinator_svr = CoordinatorServer::new(server_config).await.unwrap();
        (extractor_executor, coordinator_svr)
    }
}
