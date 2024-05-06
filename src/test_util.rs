#[cfg(test)]
pub mod db_utils {
    use std::collections::HashMap;

    use indexify_internal_api as internal_api;
    use internal_api::ContentMetadataId;
    use serde_json::json;

    pub const DEFAULT_TEST_NAMESPACE: &str = "test_namespace";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MockExtractor";

    pub fn create_metadata(val: Vec<(&str, &str)>) -> HashMap<String, serde_json::Value> {
        val.iter().map(|(k, v)| (k.to_string(), json!(v))).collect()
    }

    pub fn test_mock_content_metadata(
        id: &str,
        root_content_id: &str,
    ) -> internal_api::ContentMetadata {
        internal_api::ContentMetadata {
            id: ContentMetadataId::new(id),
            root_content_id: Some(root_content_id.to_string()),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            source: "test_source".to_string(),
            ..Default::default()
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
            input_mime_types: vec!["text/plain".to_string()],
        }
    }
}
