use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use strum_macros::{Display, EnumString};
use utoipa::ToSchema;

use crate::memory;
use crate::vectordbs;
use crate::{persistence, text_splitters};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "extractor_type")]
pub enum ExtractorType {
    #[serde(rename = "embedding")]
    Embedding {
        model: String,
        distance: IndexDistance,
        text_splitter: TextSplitterKind,
    },
}

impl From<persistence::ExtractorType> for ExtractorType {
    fn from(value: persistence::ExtractorType) -> Self {
        match value {
            persistence::ExtractorType::Embedding {
                model,
                text_splitter,
                distance,
            } => ExtractorType::Embedding {
                model,
                distance: distance.into(),
                text_splitter: text_splitter.into(),
            },
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, EnumString, Serialize, Deserialize, ToSchema, SmartDefault)]
pub enum ExtractorContentType {
    #[strum(serialize = "text")]
    #[serde(rename = "text")]
    #[default]
    Text,
}

impl From<persistence::ContentType> for ExtractorContentType {
    fn from(value: persistence::ContentType) -> Self {
        match value {
            persistence::ContentType::Text => ExtractorContentType::Text,
        }
    }
}

impl From<ExtractorContentType> for persistence::ContentType {
    fn from(val: ExtractorContentType) -> Self {
        match val {
            ExtractorContentType::Text => persistence::ContentType::Text,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumString, Display)]
#[serde(rename = "extractor_filter")]
pub enum ExtractorFilter {
    MemorySession { session_id: String },
    ContentType { content_type: ExtractorContentType },
}

impl From<persistence::ExtractorFilter> for ExtractorFilter {
    fn from(value: persistence::ExtractorFilter) -> Self {
        match value {
            persistence::ExtractorFilter::MemorySession { session_id } => {
                ExtractorFilter::MemorySession { session_id }
            }
            persistence::ExtractorFilter::ContentType { content_type } => {
                ExtractorFilter::ContentType {
                    content_type: content_type.into(),
                }
            }
        }
    }
}

impl From<ExtractorFilter> for persistence::ExtractorFilter {
    fn from(val: ExtractorFilter) -> Self {
        match val {
            ExtractorFilter::MemorySession { session_id } => {
                persistence::ExtractorFilter::MemorySession { session_id }
            }
            ExtractorFilter::ContentType { content_type } => {
                persistence::ExtractorFilter::ContentType {
                    content_type: content_type.into(),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "extractor")]
pub struct Extractor {
    pub name: String,
    pub extractor_type: ExtractorType,
    pub filter: ExtractorFilter,
}

impl From<persistence::ExtractorConfig> for Extractor {
    fn from(value: persistence::ExtractorConfig) -> Self {
        Self {
            name: value.name,
            extractor_type: value.extractor_type.into(),
            filter: value.filter.into(),
        }
    }
}

impl From<Extractor> for persistence::ExtractorConfig {
    fn from(val: Extractor) -> Self {
        persistence::ExtractorConfig {
            name: val.name,
            extractor_type: match val.extractor_type {
                ExtractorType::Embedding {
                    model,
                    distance,
                    text_splitter,
                } => persistence::ExtractorType::Embedding {
                    model,
                    distance: distance.into(),
                    text_splitter: text_splitter.into(),
                },
            },
            filter: val.filter.into(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DataRepository {
    pub name: String,
    pub extractors: Vec<Extractor>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<persistence::DataRepository> for DataRepository {
    fn from(value: persistence::DataRepository) -> Self {
        let ap_extractors = value.extractors.into_iter().map(|e| e.into()).collect();
        DataRepository {
            name: value.name,
            extractors: ap_extractors,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "source_type")]
pub enum SourceType {
    // todo: replace metadata with actual request parameters for GoogleContactApi
    #[serde(rename = "google_contact")]
    GoogleContact { metadata: Option<String> },
    // todo: replace metadata with actual request parameters for gmail API
    #[serde(rename = "gmail")]
    Gmail { metadata: Option<String> },
}

impl From<SourceType> for persistence::SourceType {
    fn from(value: SourceType) -> Self {
        match value {
            SourceType::GoogleContact { metadata } => {
                persistence::SourceType::GoogleContact { metadata }
            }
            SourceType::Gmail { metadata } => persistence::SourceType::Gmail { metadata },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "data_connector")]
pub struct DataConnector {
    pub source: SourceType,
}

impl From<DataConnector> for persistence::DataConnector {
    fn from(value: DataConnector) -> Self {
        Self {
            source: value.source.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct SyncRepository {
    pub name: String,
    pub extractors: Vec<Extractor>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub data_connectors: Vec<DataConnector>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SyncRepositoryResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepository {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepositoryResponse {
    pub repository: DataRepository,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRepositories {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRepositoriesResponse {
    pub repositories: Vec<DataRepository>,
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
pub enum TextSplitterKind {
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

impl From<text_splitters::TextSplitterKind> for TextSplitterKind {
    fn from(value: text_splitters::TextSplitterKind) -> Self {
        match value {
            text_splitters::TextSplitterKind::Noop => TextSplitterKind::None,
            text_splitters::TextSplitterKind::NewLine => TextSplitterKind::NewLine,
            text_splitters::TextSplitterKind::Regex { pattern } => {
                TextSplitterKind::Regex { pattern }
            }
        }
    }
}

impl From<TextSplitterKind> for text_splitters::TextSplitterKind {
    fn from(val: TextSplitterKind) -> Self {
        match val {
            TextSplitterKind::None => text_splitters::TextSplitterKind::Noop,
            TextSplitterKind::NewLine => text_splitters::TextSplitterKind::NewLine,
            TextSplitterKind::Regex { pattern } => {
                text_splitters::TextSplitterKind::Regex { pattern }
            }
        }
    }
}

#[derive(Display, Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
#[serde(rename = "distance")]
pub enum IndexDistance {
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

impl From<IndexDistance> for vectordbs::IndexDistance {
    fn from(value: IndexDistance) -> Self {
        match value {
            IndexDistance::Dot => vectordbs::IndexDistance::Dot,
            IndexDistance::Cosine => vectordbs::IndexDistance::Cosine,
            IndexDistance::Euclidean => vectordbs::IndexDistance::Euclidean,
        }
    }
}

impl From<vectordbs::IndexDistance> for IndexDistance {
    fn from(val: vectordbs::IndexDistance) -> Self {
        match val {
            vectordbs::IndexDistance::Dot => IndexDistance::Dot,
            vectordbs::IndexDistance::Cosine => IndexDistance::Cosine,
            vectordbs::IndexDistance::Euclidean => IndexDistance::Euclidean,
        }
    }
}

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractorAddRequest {
    pub repository: Option<String>,
    pub extractor: Extractor,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ExtractorAddResponse {}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Text {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TextAddRequest {
    pub repository: Option<String>,
    pub documents: Vec<Text>,
    pub sync: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunExtractors {
    pub repository: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunExtractorsResponse {}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct IndexAdditionResponse {
    pub sequence: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateMemorySessionRequest {
    pub session_id: Option<String>,
    pub repository: Option<String>,
    pub extractor: Option<Extractor>,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateMemorySessionResponse {
    pub session_id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Message {
    text: String,
    role: String,
    #[serde(default)]
    unix_timestamp: u64,
    metadata: HashMap<String, serde_json::Value>,
}

impl From<Message> for memory::Message {
    fn from(value: Message) -> Self {
        memory::Message::new(&value.text, &value.role, value.metadata)
    }
}

impl From<memory::Message> for Message {
    fn from(value: memory::Message) -> Self {
        Self {
            text: value.text,
            role: value.role,
            unix_timestamp: value.unix_timestamp,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemorySessionAddRequest {
    pub session_id: String,
    pub repository: Option<String>,
    pub messages: Vec<Message>,
    pub sync: Option<bool>,
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

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct DocumentFragment {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct IndexSearchResponse {
    pub results: Vec<DocumentFragment>,
}
pub struct IndexifyAPIError {
    status_code: StatusCode,
    message: String,
}

impl IndexifyAPIError {
    pub fn new(status_code: StatusCode, message: String) -> Self {
        Self {
            status_code,
            message,
        }
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        (self.status_code, self.message).into_response()
    }
}
