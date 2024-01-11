#[cfg(test)]
pub mod db_utils {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::internal_api::{EmbeddingSchema, ExtractorDescription, OutputSchema};

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
}
