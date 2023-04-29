use crate::index::Index;
use crate::{vectordbs, CreateIndexParams, EmbeddingRouter, MetricKind, ServerConfig, VectorDBTS};

use super::embeddings::EmbeddingGenerator;
use anyhow::Result;
use axum::http::StatusCode;
use axum::{extract::State, routing::get, routing::post, Json, Router};

use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use std::collections::HashMap;

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

#[derive(SmartDefault, Debug, Serialize, Deserialize)]
enum TextSplitterKind {
    #[default]
    NewLine,
    Html {
        #[default = 1]
        num_elements: u64,
    },
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
struct Document {
    text: String,
    metadata: HashMap<String, String>,
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
                "/index/create",
                post(index_create).with_state((vectordb.clone(), embedding_router.clone())),
            )
            .route(
                "/index/add",
                post(add_texts).with_state((vectordb.clone(), embedding_router.clone())),
            )
            .route(
                "/index/search",
                get(index_search).with_state((vectordb.clone(), embedding_router.clone())),
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
    let result = Index::create_index(index_params, index_args.0.unwrap()).await;
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
    State(index_args): State<(Option<VectorDBTS>, Arc<EmbeddingRouter>)>,
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
    let vectordb = index_args.0.unwrap();
    let index = Index::new(vectordb, index_args.1, "all-minilm-l12-v2".into()).await;

    if let Err(err) = index {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexAdditionResponse {
                errors: vec![err.to_string()],
            }),
        );
    }
    let mut texts: Vec<String> = Vec::new();
    let mut attrs: Vec<HashMap<String, String>> = Vec::new();
    for document in payload.texts {
        texts.push(document.text);
        attrs.push(document.metadata);
    }
    let result = index.unwrap().add_texts(payload.index, texts, attrs).await;
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
    State(index_args): State<(Option<VectorDBTS>, Arc<EmbeddingRouter>)>,
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
    let vectordb = index_args.0.unwrap();
    let index = Index::new(vectordb, index_args.1, "all-minilm-l12-v2".into()).await;

    if let Err(err) = index {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(IndexSearchResponse {
                errors: vec![err.to_string()],
                ..Default::default()
            }),
        );
    }
    let results = index
        .unwrap()
        .search(
            "all-minilm-l12-v2".into(),
            query.query,
            query.index,
            query.k,
        )
        .await;
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
