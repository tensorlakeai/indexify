use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedQueryRequest {
    pub extractor_name: String,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedQueryResponse {
    pub embedding: Vec<f32>,
}
