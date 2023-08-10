use std::collections::HashMap;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use strum_macros::{Display, EnumString};
use utoipa::ToSchema;

use crate::memory;
use crate::persistence;
use crate::vectordbs;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename = "extractor_type")]
pub enum ExtractorType {
    #[serde(rename = "embedding")]
    Embedding { dim: usize, distance: IndexDistance },

    #[serde(rename = "embedding")]
    Attributes { schema: String },
}

impl From<persistence::ExtractorType> for ExtractorType {
    fn from(value: persistence::ExtractorType) -> Self {
        match value {
            persistence::ExtractorType::Embedding { dim, distance } => ExtractorType::Embedding {
                dim,
                distance: distance.into(),
            },
            persistence::ExtractorType::Attributes { schema } => {
                ExtractorType::Attributes { schema }
            }
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
#[serde(untagged)]
pub enum ExtractorFilter {
    #[serde(rename = "memory_session_id")]
    MemorySession { session_id: String },

    #[serde(rename = "content_type")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorBinding {
    pub name: String,
    pub index_name: Option<String>,
    pub filter: ExtractorFilter,
    pub input_params: Option<serde_json::Value>,
}

impl From<persistence::ExtractorBinding> for ExtractorBinding {
    fn from(value: persistence::ExtractorBinding) -> Self {
        Self {
            name: value.extractor_name,
            index_name: Some(value.index_name),
            filter: value.filter.into(),
            input_params: Some(value.input_params),
        }
    }
}

impl From<ExtractorBinding> for persistence::ExtractorBinding {
    fn from(val: ExtractorBinding) -> Self {
        persistence::ExtractorBinding {
            extractor_name: val.name.clone(),
            index_name: val.index_name.unwrap_or(val.name.clone()),
            filter: val.filter.into(),
            input_params: val.input_params.unwrap_or(serde_json::json!({})),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct DataRepository {
    pub name: String,
    pub extractors: Vec<ExtractorBinding>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<persistence::DataRepository> for DataRepository {
    fn from(value: persistence::DataRepository) -> Self {
        let ap_extractors = value
            .extractor_bindings
            .into_iter()
            .map(|e| e.into())
            .collect();
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
pub struct CreateRepository {
    pub name: String,
    pub extractors: Vec<ExtractorBinding>,
    pub metadata: HashMap<String, serde_json::Value>,
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
    #[serde(flatten)]
    pub extractor_binding: ExtractorBinding,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractorConfig {
    pub name: String,
    pub description: String,
    pub extractor_type: ExtractorType,
}

impl From<persistence::ExtractorConfig> for ExtractorConfig {
    fn from(value: persistence::ExtractorConfig) -> Self {
        Self {
            name: value.name,
            description: value.description,
            extractor_type: value.extractor_type.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListExtractorsResponse {
    pub extractors: Vec<ExtractorConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct IndexAdditionResponse {
    pub sequence: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchRequest {
    pub repository: String,
    pub index: String,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedAttributes {
    pub id: String,
    pub content_id: String,
    pub attributes: serde_json::Value,
    pub extractor_name: String,
}

impl From<persistence::ExtractedAttributes> for ExtractedAttributes {
    fn from(value: persistence::ExtractedAttributes) -> Self {
        Self {
            id: value.id,
            content_id: value.content_id,
            attributes: value.attributes,
            extractor_name: value.extractor_name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AttributeLookupRequest {
    pub repository: String,
    pub content_id: Option<String>,
    pub index: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AttributeLookupResponse {
    pub attributes: Vec<ExtractedAttributes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateMemorySessionRequest {
    pub session_id: Option<String>,
    pub repository: Option<String>,
    pub extractor_binding: Option<ExtractorBinding>,
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

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct DocumentFragment {
    pub text: String,
    pub confidence_score: f32,
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
