#[cfg(test)]
pub mod db_utils {
    use std::collections::HashMap;

    use indexify_internal_api as internal_api;
    use internal_api::{ContentMetadataId, ExtractionGraph, ExtractionPolicy};
    use serde_json::json;

    pub const DEFAULT_TEST_NAMESPACE: &str = "test_namespace";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MockExtractor";

    pub fn create_metadata(val: Vec<(&str, &str)>) -> HashMap<String, serde_json::Value> {
        val.iter().map(|(k, v)| (k.to_string(), json!(v))).collect()
    }

    pub fn test_mock_content_metadata(
        id: &str,
        root_content_id: &str,
        graph_name: &str,
    ) -> internal_api::ContentMetadata {
        internal_api::ContentMetadata {
            id: ContentMetadataId::new(id),
            root_content_id: Some(root_content_id.to_string()),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extraction_graph_names: vec![graph_name.to_string()],
            ..Default::default()
        }
    }

    pub fn create_test_extraction_graph(
        graph_name: &str,
        extraction_policy_names: Vec<&str>,
    ) -> ExtractionGraph {
        let id = ExtractionGraph::create_id(graph_name, DEFAULT_TEST_NAMESPACE);

        let mut extraction_policies = Vec::new();
        for policy_name in extraction_policy_names {
            let id = ExtractionPolicy::create_id(graph_name, policy_name, DEFAULT_TEST_NAMESPACE);
            let ep = ExtractionPolicy {
                id,
                graph_name: graph_name.to_string(),
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                name: policy_name.to_string(),
                extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                input_params: json!({}),
                filters: HashMap::new(),
                output_table_mapping: HashMap::from([(
                    "test_output".to_string(),
                    "test_table".to_string(),
                )]),
                content_source: internal_api::ExtractionPolicyContentSource::Ingestion,
            };
            extraction_policies.push(ep);
        }
        ExtractionGraph {
            id,
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            name: graph_name.to_string(),
            extraction_policies,
        }
    }

    pub fn mock_extractor() -> internal_api::ExtractorDescription {
        let mut outputs = HashMap::new();
        outputs.insert(
            "test_output".to_string(),
            internal_api::OutputSchema::Embedding(internal_api::EmbeddingSchema {
                dim: 384,
                distance: "cosine".to_string(),
            }),
        );
        internal_api::ExtractorDescription {
            name: DEFAULT_TEST_EXTRACTOR.to_string(),
            description: "test_description".to_string(),
            input_params: json!({}),
            outputs,
            input_mime_types: vec!["*/*".to_string()],
        }
    }

    pub fn mock_extractors() -> Vec<internal_api::ExtractorDescription> {
        vec![mock_extractor()]
    }
}
