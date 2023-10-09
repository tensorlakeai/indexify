use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct EmbedQueryRequest {
    pub extractor_name: String,
    pub text: String,
}

#[derive(Serialize, Deserialize)]
pub struct EmbedQueryResponse {
    pub embedding: Vec<f32>,
}
