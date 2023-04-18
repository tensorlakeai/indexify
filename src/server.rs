use crate::server_config;

use super::embedding::EmbeddingGenerator;
use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Server {
    addr: SocketAddr,
    available_models: Vec<server_config::SentenceEmbeddingModels>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingRequest {
    inputs: Vec<String>,
    model: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListEmbeddingModelsResponse {
    models: Vec<String>,
}

impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr.parse()?;
        Ok(Self {
            addr,
            available_models: config.available_models.clone(),
        })
    }

    pub async fn run(&self) -> Result<()> {
        let embedding_generator = Arc::new(EmbeddingGenerator::new(self.available_models.clone())?);
        let app = Router::new()
            .route("/", get(root))
            .route("/embeddings/models", get(list_embedding_models))
            .route(
                "/embeddings/generate",
                get(generate_embedding).with_state(embedding_generator),
            );

        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    }
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    // TODO - List the routes enabled
    "Indexify Server"
}

async fn list_embedding_models() -> Json<ListEmbeddingModelsResponse> {
    Json(ListEmbeddingModelsResponse {
        models: vec!["all-mini-lm-l12-v2".to_string(), "openai-text-ada-03".to_string()],
    })
}

#[axum_macros::debug_handler]
async fn generate_embedding(
    State(embedding_generator): State<Arc<EmbeddingGenerator>>,
    Json(payload): Json<GenerateEmbeddingRequest>,
) -> Json<GenerateEmbeddingResponse> {
    let embeddings = embedding_generator
        .generate_embeddings(payload.inputs, payload.model)
        .await
        .unwrap();
    Json(GenerateEmbeddingResponse {
        embeddings,
    })
}
