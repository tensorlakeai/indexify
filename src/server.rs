use crate::index::Index;
use crate::{vectordbs, CreateIndexParams, EmbeddingRouter, MetricKind, ServerConfig, VectorDBTS};

use super::embeddings::EmbeddingGenerator;
use anyhow::Result;
use axum::http::StatusCode;
use axum::{extract::State, routing::get, routing::post, Json, Router};

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;


pub struct Server {
    addr: SocketAddr,
    config: Arc<ServerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingRequest {
    inputs: Vec<String>,
    model: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingResponse {
    embeddings: Option<Vec<Vec<f32>>>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmbeddingModel {
    name: String,
    dimensions: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListEmbeddingModelsResponse {
    models: Vec<EmbeddingModel>,
}

#[derive(Debug, Serialize, Deserialize)]
enum TextSplitterKind {
    NewLine,
    Html { num_elements: i32 },
}

#[derive(Debug, Serialize, Deserialize)]
enum IndexMetric {
    Dot,
    Cosine,
    Euclidean,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexCreateRequest {
    name: String,
    embedding_model: String,
    metric: IndexMetric,
    text_splitter: TextSplitterKind,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexCreateResponse {}

impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr.parse()?;
        Ok(Self { addr, config })
    }

    pub async fn run(&self) -> Result<()> {
        let embedding_router = Arc::new(EmbeddingRouter::new(self.config.clone())?);
        let vectordb = vectordbs::create_vectordb(self.config.clone())?;
        let app = Router::new()
            .route("/", get(root))
            .route(
                "/embeddings/models",
                get(list_embedding_models).with_state(embedding_router.clone()),
            )
            .route(
                "/embeddings/generate",
                get(generate_embedding).with_state(embedding_router.clone()),
            )
            .route(
                "index/create",
                post(index_create).with_state((vectordb, embedding_router)),
            );

        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .await?;
        Ok(())
    }
}

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Indexify Server"
}

#[axum_macros::debug_handler]
async fn index_create(
    State(index_args): State<(Option<VectorDBTS>, Arc<EmbeddingRouter>)>,
    Json(payload): Json<IndexCreateRequest>,
) -> (StatusCode, Json<IndexCreateResponse>) {
    if index_args.0.is_none() {
        return (StatusCode::BAD_REQUEST, Json(IndexCreateResponse {}));
    }
    let try_dim = index_args.1.dimensions(payload.embedding_model.clone());
    if let Err(_err) = try_dim {
        return (StatusCode::BAD_REQUEST, Json(IndexCreateResponse {}));
    }
    let index_params = CreateIndexParams {
        name: payload.name.clone(),
        vector_dim: try_dim.unwrap(),
        metric: match payload.metric {
            IndexMetric::Cosine => MetricKind::Cosine,
            IndexMetric::Dot => MetricKind::Dot,
            IndexMetric::Euclidean => MetricKind::Euclidean,
        },
    };
    let index = Index::new(
        index_params,
        index_args.0.unwrap(),
        index_args.1,
        payload.embedding_model.clone(),
    )
    .await;
    if let Err(_err) = index {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexCreateResponse {}),
        );
    }
    (StatusCode::OK, Json(IndexCreateResponse {}))
}

#[axum_macros::debug_handler]
async fn list_embedding_models(
    State(embedding_router): State<Arc<EmbeddingRouter>>,
) -> Json<ListEmbeddingModelsResponse> {
    let model_names = embedding_router.list_models();
    let mut models: Vec<EmbeddingModel> = Vec::new();
    for model in model_names {
        if let Ok(dimensions) = embedding_router.dimensions(model.clone()) {
            models.push(EmbeddingModel {
                name: model.clone(),
                dimensions,
            })
        }
    }
    Json(ListEmbeddingModelsResponse { models })
}

#[axum_macros::debug_handler]
async fn generate_embedding(
    State(embedding_generator): State<Arc<dyn EmbeddingGenerator + Sync + Send>>,
    Json(payload): Json<GenerateEmbeddingRequest>,
) -> (StatusCode, Json<GenerateEmbeddingResponse>) {
    let embeddings = embedding_generator
        .generate_embeddings(payload.inputs, payload.model)
        .await;

    if let Err(err) = embeddings {
        return (
            StatusCode::EXPECTATION_FAILED,
            Json(GenerateEmbeddingResponse {
                embeddings: None,
                error: Some(err.to_string()),
            }),
        );
    }

    (
        StatusCode::OK,
        Json(GenerateEmbeddingResponse {
            embeddings: Some(embeddings.unwrap()),
            error: None,
        }),
    )
}
