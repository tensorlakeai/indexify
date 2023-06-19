use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use strum_macros::{Display, EnumString};
use utoipa::ToSchema;

use crate::{
    persistence::{
        ContentType, DataConnector, DataRepository, ExtractorConfig, ExtractorType, SourceType,
    },
    text_splitters::TextSplitterKind,
    IndexDistance, Message,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "extractor_type")]
pub enum ApiExtractorType {
    #[serde(rename = "embedding")]
    Embedding {
        model: String,
        distance: ApiIndexDistance,
        text_splitter: ApiTextSplitterKind,
    },
}

impl From<ExtractorType> for ApiExtractorType {
    fn from(value: ExtractorType) -> Self {
        match value {
            ExtractorType::Embedding {
                model,
                text_splitter,
                distance,
            } => ApiExtractorType::Embedding {
                model,
                distance: distance.into(),
                text_splitter: text_splitter.into(),
            },
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, EnumString, Serialize, Deserialize, ToSchema)]
pub enum ApiExtractorContentType {
    #[strum(serialize = "text")]
    #[serde(rename = "text")]
    Text,

    #[strum(serialize = "memory")]
    #[serde(rename = "memory")]
    Memory,
}

impl From<ContentType> for ApiExtractorContentType {
    fn from(value: ContentType) -> Self {
        match value {
            ContentType::Text => ApiExtractorContentType::Text,
            ContentType::Memory => ApiExtractorContentType::Memory,
        }
    }
}

impl From<ApiExtractorContentType> for ContentType {
    fn from(val: ApiExtractorContentType) -> Self {
        match val {
            ApiExtractorContentType::Text => ContentType::Text,
            ApiExtractorContentType::Memory => ContentType::Memory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "extractor")]
pub struct ApiExtractor {
    pub name: String,
    pub extractor_type: ApiExtractorType,
    pub content_type: ApiExtractorContentType,
}

impl From<ExtractorConfig> for ApiExtractor {
    fn from(value: ExtractorConfig) -> Self {
        Self {
            name: value.name,
            extractor_type: value.extractor_type.into(),
            content_type: value.content_type.into(),
        }
    }
}

impl From<ApiExtractor> for ExtractorConfig {
    fn from(val: ApiExtractor) -> Self {
        ExtractorConfig {
            name: val.name,
            extractor_type: match val.extractor_type {
                ApiExtractorType::Embedding {
                    model,
                    distance,
                    text_splitter,
                } => ExtractorType::Embedding {
                    model,
                    distance: distance.into(),
                    text_splitter: text_splitter.into(),
                },
            },
            content_type: val.content_type.into(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ApiDataRepository {
    pub name: String,
    pub extractors: Vec<ApiExtractor>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<DataRepository> for ApiDataRepository {
    fn from(value: DataRepository) -> Self {
        let ap_extractors = value.extractors.into_iter().map(|e| e.into()).collect();
        ApiDataRepository {
            name: value.name,
            extractors: ap_extractors,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "source_type")]
pub enum ApiSourceType {
    // todo: replace metadata with actual request parameters for GoogleContactApi
    #[serde(rename = "google_contact")]
    GoogleContact { metadata: Option<String> },
    // todo: replace metadata with actual request parameters for gmail API
    #[serde(rename = "gmail")]
    Gmail { metadata: Option<String> },
}

impl From<ApiSourceType> for SourceType {
    fn from(value: ApiSourceType) -> Self {
        match value {
            ApiSourceType::GoogleContact { metadata } => SourceType::GoogleContact { metadata },
            ApiSourceType::Gmail { metadata } => SourceType::Gmail { metadata },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "data_connector")]
pub struct ApiDataConnector {
    pub source: ApiSourceType,
}

impl From<ApiDataConnector> for DataConnector {
    fn from(value: ApiDataConnector) -> Self {
        Self {
            source: value.source.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct SyncRepository {
    pub name: String,
    pub extractors: Vec<ApiExtractor>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub data_connectors: Vec<ApiDataConnector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SyncRepositoryResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepository {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepositoryResponse {
    pub repository: ApiDataRepository,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRepositories {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRepositoriesResponse {
    pub repositories: Vec<ApiDataRepository>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateEmbeddingRequest {
    /// Input texts for which embeddings will be generated.
    pub inputs: Vec<String>,
    /// Name of the model to use for generating embeddings.
    pub model: String,
}

/// Response payload for generating text embeddings.
#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateEmbeddingResponse {
    pub embeddings: Option<Vec<Vec<f32>>>,
}

/// An embedding model and its properties.
#[derive(Debug, Serialize, Deserialize)]
pub struct EmbeddingModel {
    /// Name of the embedding model.
    pub name: String,
    /// Number of dimensions in the embeddings generated by this model.
    pub dimensions: u64,
}

/// Response payload for listing available embedding models.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListEmbeddingModelsResponse {
    /// List of available embedding models.
    pub models: Vec<EmbeddingModel>,
}

#[derive(SmartDefault, Debug, Serialize, Deserialize, strum::Display, Clone, ToSchema)]
#[strum(serialize_all = "snake_case")]
pub enum ApiTextSplitterKind {
    // Do not split text.
    #[serde(rename = "none")]
    None,

    /// Split text by new lines.
    #[default]
    #[serde(rename = "new_line")]
    NewLine,

    /// Split a document across the regex boundary
    #[serde(rename = "regex")]
    Regex { pattern: String },
}

impl From<TextSplitterKind> for ApiTextSplitterKind {
    fn from(value: TextSplitterKind) -> Self {
        match value {
            TextSplitterKind::Noop => ApiTextSplitterKind::None,
            TextSplitterKind::NewLine => ApiTextSplitterKind::NewLine,
            TextSplitterKind::Regex { pattern } => ApiTextSplitterKind::Regex { pattern },
        }
    }
}

impl From<ApiTextSplitterKind> for TextSplitterKind {
    fn from(val: ApiTextSplitterKind) -> Self {
        match val {
            ApiTextSplitterKind::None => TextSplitterKind::Noop,
            ApiTextSplitterKind::NewLine => TextSplitterKind::NewLine,
            ApiTextSplitterKind::Regex { pattern } => TextSplitterKind::Regex { pattern },
        }
    }
}

#[derive(Display, Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
#[serde(rename = "distance")]
pub enum ApiIndexDistance {
    #[serde(rename = "dot")]
    #[strum(serialize = "dot")]
    #[default]
    Dot,

    #[serde(rename = "cosine")]
    #[strum(serialize = "cosine")]
    Cosine,

    #[serde(rename = "euclidean")]
    #[strum(serialize = "euclidean")]
    Euclidean,
}

impl From<ApiIndexDistance> for IndexDistance {
    fn from(value: ApiIndexDistance) -> Self {
        match value {
            ApiIndexDistance::Dot => IndexDistance::Dot,
            ApiIndexDistance::Cosine => IndexDistance::Cosine,
            ApiIndexDistance::Euclidean => IndexDistance::Euclidean,
        }
    }
}

impl From<IndexDistance> for ApiIndexDistance {
    fn from(val: IndexDistance) -> Self {
        match val {
            IndexDistance::Dot => ApiIndexDistance::Dot,
            IndexDistance::Cosine => ApiIndexDistance::Cosine,
            IndexDistance::Euclidean => ApiIndexDistance::Euclidean,
        }
    }
}

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractorAddRequest {
    pub repository: Option<String>,
    pub extractor: ApiExtractor,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ExtractorAddResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiText {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TextAddRequest {
    pub repository: Option<String>,
    pub documents: Vec<ApiText>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IndexAdditionResponse {
    pub sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateMemorySessionRequest {
    pub session_id: Option<String>,
    pub repository: Option<String>,
    pub extractor: Option<ApiExtractor>,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateMemorySessionResponse {
    pub session_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemorySessionAddRequest {
    pub session_id: String,
    pub repository: Option<String>,
    pub messages: Vec<Message>,
}

#[derive(Serialize, Deserialize)]
pub struct MemorySessionAddResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemorySessionRetrieveRequest {
    pub session_id: String,
    pub repository: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct MemorySessionRetrieveResponse {
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemorySessionSearchRequest {
    pub session_id: String,
    pub repository: Option<String>,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct MemorySessionSearchResponse {
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DocumentFragment {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IndexSearchResponse {
    pub results: Vec<DocumentFragment>,
}
