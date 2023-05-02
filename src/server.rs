use crate::index::{IndexManager, Text};
use crate::{CreateIndexParams, EmbeddingRouter, MetricKind, ServerConfig};

use super::embeddings::EmbeddingGenerator;
use anyhow::Result;
use axum::http::StatusCode;
use axum::{extract::State, routing::get, routing::post, Json, Router};
use tracing::info;

use core::fmt;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use std::collections::HashMap;

use std::net::SocketAddr;
use std::sync::Arc;

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

#[derive(SmartDefault, Debug, Serialize, Deserialize)]
enum TextSplitterKind {
    #[default]
    NewLine,
    Html {
        #[default = 1]
        num_elements: u64,
    },
}

impl fmt::Display for TextSplitterKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TextSplitterKind::NewLine => write!(f, "new_line"),
            TextSplitterKind::Html { num_elements: _ } => write!(f, "html"),
        }
    }
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

#[derive(Debug, Serialize, Deserialize, Default)]
struct IndexCreateResponse {
    errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Document {
    pub text: String,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddTextsRequest {
    index: String,
    texts: Vec<Document>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct IndexAdditionResponse {
    errors: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SearchRequest {
    index: String,
    query: String,
    k: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DocumentFragment {
    text: String,
    source: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct IndexSearchResponse {
    results: Vec<DocumentFragment>,
    errors: Vec<String>,
}

type IndexEndpointState = (Arc<Option<IndexManager>>, Arc<EmbeddingRouter>);

pub struct Server {
    addr: SocketAddr,
    config: Arc<ServerConfig>,
}
impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr.parse()?;
        Ok(Self { addr, config })
    }

    pub async fn run(&self) -> Result<()> {
        let embedding_router = Arc::new(EmbeddingRouter::new(self.config.clone())?);
        let index_manager = Arc::new(
            IndexManager::new(self.config.index_config.clone(), embedding_router.clone()).await?,
        );
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
                "/index/create",
                post(index_create).with_state((index_manager.clone(), embedding_router.clone())),
            )
            .route(
                "/index/add",
                post(add_texts).with_state((index_manager.clone(), embedding_router.clone())),
            )
            .route(
                "/index/search",
                get(index_search).with_state((index_manager.clone(), embedding_router.clone())),
            );

        info!("server is listening at addr {:?}", &self.addr.to_string());
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
    State(index_args): State<IndexEndpointState>,
    Json(payload): Json<IndexCreateRequest>,
) -> (StatusCode, Json<IndexCreateResponse>) {
    if index_args.0.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexCreateResponse {
                errors: vec!["server is not configured to have indexes".into()],
            }),
        );
    }
    let try_dim = index_args.1.dimensions(payload.embedding_model.clone());
    if let Err(err) = try_dim {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexCreateResponse {
                errors: vec![err.to_string()],
            }),
        );
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
    let index_manager = index_args.0.as_ref();
    let result = index_manager
        .as_ref()
        .unwrap()
        .create_index(
            index_params,
            payload.embedding_model,
            payload.text_splitter.to_string(),
        )
        .await;
    if let Err(err) = result {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexCreateResponse {
                errors: vec![err.to_string()],
            }),
        );
    }
    (StatusCode::OK, Json(IndexCreateResponse { errors: vec![] }))
}

#[axum_macros::debug_handler]
async fn add_texts(
    State(index_args): State<IndexEndpointState>,
    Json(payload): Json<AddTextsRequest>,
) -> (StatusCode, Json<IndexAdditionResponse>) {
    if index_args.0.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexAdditionResponse {
                errors: vec!["server is not configured to have indexes".into()],
            }),
        );
    }
    let index_manager = index_args.0.as_ref().as_ref().unwrap();
    let try_index = index_manager.load(payload.index).await;
    if let Err(err) = try_index {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexAdditionResponse {
                errors: vec![err.to_string()],
            }),
        );
    }
    if try_index.as_ref().unwrap().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexAdditionResponse {
                errors: vec!["index does not exist".into()],
            }),
        );
    }
    let index = try_index.unwrap().unwrap();
    let texts = payload
        .texts
        .iter()
        .map(|d| Text {
            text: d.text.to_owned(),
            metadata: d.metadata.to_owned(),
        })
        .collect();
    let result = index.add_texts(texts).await;
    if let Err(err) = result {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexAdditionResponse {
                errors: vec![err.to_string()],
            }),
        );
    }

    (StatusCode::OK, Json(IndexAdditionResponse::default()))
}

#[axum_macros::debug_handler]
async fn index_search(
    State(index_args): State<IndexEndpointState>,
    Json(query): Json<SearchRequest>,
) -> (StatusCode, Json<IndexSearchResponse>) {
    if index_args.0.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexSearchResponse {
                errors: vec!["server is not configured to have indexes".into()],
                ..Default::default()
            }),
        );
    }

    let index_manager = index_args.0.as_ref().as_ref().unwrap();
    let try_index = index_manager.load(query.index.clone()).await;
    if let Err(err) = try_index {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexSearchResponse {
                results: vec![],
                errors: vec![err.to_string()],
            }),
        );
    }
    if try_index.as_ref().unwrap().is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(IndexSearchResponse {
                results: vec![],
                errors: vec!["index does not exist".into()],
            }),
        );
    }
    let index = try_index.unwrap().unwrap();
    let results = index.search(query.index, query.query, query.k).await;
    if let Err(err) = results {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexSearchResponse {
                results: vec![],
                errors: vec![err.to_string()],
            }),
        );
    }
    let document_fragments: Vec<DocumentFragment> = results
        .unwrap()
        .iter()
        .map(|text| DocumentFragment {
            text: text.to_owned(),
            source: HashMap::new(),
        })
        .collect();
    (
        StatusCode::OK,
        Json(IndexSearchResponse {
            results: document_fragments,
            errors: vec![],
        }),
    )
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
