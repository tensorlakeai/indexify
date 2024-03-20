use std::collections::HashMap;

use anyhow::Result;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};
use utoipa::{IntoParams, ToSchema};

use crate::{api_utils, metadata_storage, vectordbs};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractionPolicy {
    pub extractor: String,
    pub name: String,
    #[serde(default, deserialize_with = "api_utils::deserialize_labels_eq_filter")]
    pub filters_eq: Option<HashMap<String, String>>,
    pub input_params: Option<serde_json::Value>,
    pub content_source: Option<String>,
}

impl From<ExtractionPolicy> for indexify_coordinator::ExtractionPolicy {
    fn from(value: ExtractionPolicy) -> Self {
        Self {
            extractor: value.extractor,
            name: value.name,
            filters: value.filters_eq.unwrap_or_default(),
            input_params: value
                .input_params
                .map(|v| v.to_string())
                .unwrap_or("{}".to_string()),
            content_source: value.content_source.unwrap_or("ingestion".to_string()),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataNamespace {
    pub name: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl TryFrom<indexify_coordinator::Namespace> for DataNamespace {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Namespace) -> Result<Self> {
        let mut extraction_policies = Vec::new();
        for policies in value.policies {
            extraction_policies.push(ExtractionPolicy {
                extractor: policies.extractor,
                name: policies.name,
                filters_eq: Some(policies.filters),
                input_params: Some(serde_json::from_str(&policies.input_params)?),
                content_source: Some(policies.content_source),
            });
        }
        Ok(Self {
            name: value.name,
            extraction_policies,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct CreateNamespace {
    pub name: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateNamespaceResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetNamespaceResponse {
    pub namespace: DataNamespace,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<DataNamespace>,
}

#[derive(Display, EnumString, Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
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
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExtractionPolicyRequest {
    #[serde(flatten)]
    pub policy: ExtractionPolicy,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ExtractionPolicyResponse {
    #[serde(default)]
    pub index_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Text {
    pub text: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TextAddRequest {
    pub documents: Vec<Text>,
    pub sync: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunExtractorsResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EmbeddingSchema {
    pub dim: usize,
    pub distance: IndexDistance,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, ToSchema)]
pub enum ExtractorOutputSchema {
    #[serde(rename = "embedding")]
    Embedding(EmbeddingSchema),
    #[serde(rename = "metadata")]
    Metadata(serde_json::Value),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractorDescription {
    pub name: String,
    pub input_mime_types: Vec<String>,
    pub description: String,
    pub input_params: serde_json::Value,
    pub outputs: HashMap<String, ExtractorOutputSchema>,
}

impl TryFrom<indexify_coordinator::Extractor> for ExtractorDescription {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Extractor) -> Result<Self> {
        let mut outputs = HashMap::new();
        for (k, v) in value.embedding_schemas.iter() {
            let schema: EmbeddingSchema = serde_json::from_str(v)?;
            outputs.insert(k.clone(), ExtractorOutputSchema::Embedding(schema));
        }
        for (k, v) in value.metadata_schemas.iter() {
            outputs.insert(
                k.clone(),
                ExtractorOutputSchema::Metadata(serde_json::from_str(v)?),
            );
        }
        Ok(Self {
            name: value.name,
            description: value.description,
            input_params: serde_json::from_str(&value.input_params).unwrap(),
            outputs,
            input_mime_types: value.input_mime_types,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Executor {
    pub id: String,
    pub extractors: Vec<ExtractorDescription>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListExecutorsResponse {
    pub executors: Vec<Executor>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ListExtractorsResponse {
    pub extractors: Vec<ExtractorDescription>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct TextAdditionResponse {}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Index {
    pub name: String,
    pub schema: ExtractorOutputSchema,
}

impl TryFrom<indexify_coordinator::Index> for Index {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Index) -> Result<Self> {
        Ok(Self {
            name: value.name,
            schema: serde_json::from_str(&value.schema).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListIndexesResponse {
    pub indexes: Vec<Index>,
}

#[derive(Debug, Serialize, Deserialize, IntoParams, ToSchema)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractedMetadata {
    pub id: String,
    pub content_id: String,
    pub metadata: serde_json::Value,
    pub extractor_name: String,
}

impl From<metadata_storage::ExtractedMetadata> for ExtractedMetadata {
    fn from(value: metadata_storage::ExtractedMetadata) -> Self {
        Self {
            id: value.id,
            content_id: value.content_id,
            metadata: value.metadata,
            extractor_name: value.extractor_name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Clone)]
pub struct ListContentFilters {
    #[serde(
        deserialize_with = "api_utils::deserialize_none_to_empty_string",
        default
    )]
    pub source: String,
    #[serde(
        deserialize_with = "api_utils::deserialize_none_to_empty_string",
        default
    )]
    pub parent_id: String,
    #[serde(default, deserialize_with = "api_utils::deserialize_labels_eq_filter")]
    pub labels_eq: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, IntoParams, ToSchema)]
pub struct MetadataRequest {
    pub content_id: String,
    pub index: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MetadataResponse {
    pub metadata: Vec<ExtractedMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct DocumentFragment {
    pub content_id: String,
    pub text: String,
    pub mime_type: String,
    pub confidence_score: f32,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct IndexSearchResponse {
    pub results: Vec<DocumentFragment>,
}

#[derive(Debug)]
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
        tracing::error!("API Error: {} - {}", self.status_code, self.message);
        (self.status_code, self.message).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]

pub struct ListContentResponse {
    pub content_list: Vec<ContentMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema, Clone)]
pub struct ContentMetadata {
    pub id: String,
    pub parent_id: String,
    pub namespace: String,
    pub name: String,
    pub mime_type: String,
    pub labels: HashMap<String, String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: String,
    pub size: u64,
}

impl From<indexify_coordinator::ContentMetadata> for ContentMetadata {
    fn from(value: indexify_coordinator::ContentMetadata) -> Self {
        Self {
            id: value.id,
            parent_id: value.parent_id,
            namespace: value.namespace,
            name: value.file_name,
            mime_type: value.mime,
            labels: value.labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            source: value.source,
            size: value.size_bytes,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString, ToSchema)]
pub enum FeatureType {
    #[serde(rename = "embedding")]
    Embedding,
    #[serde(rename = "metadata")]
    Metadata,
    #[serde(rename = "unknown")]
    Unknown,
}

impl From<internal_api::FeatureType> for FeatureType {
    fn from(feature_type: internal_api::FeatureType) -> Self {
        match feature_type {
            internal_api::FeatureType::Embedding => FeatureType::Embedding,
            internal_api::FeatureType::Metadata => FeatureType::Metadata,
            internal_api::FeatureType::Unknown => FeatureType::Unknown,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Feature {
    pub feature_type: FeatureType,
    pub name: String,
    pub data: serde_json::Value,
}

impl From<internal_api::Feature> for Feature {
    fn from(feature: internal_api::Feature) -> Self {
        Self {
            feature_type: feature.feature_type.into(),
            name: feature.name,
            data: feature.data,
        }
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Content {
    pub content_type: String,
    #[serde_as(as = "BytesOrString")]
    pub bytes: Vec<u8>,
    pub features: Vec<Feature>,
    pub labels: HashMap<String, String>,
}

impl From<internal_api::Content> for Content {
    fn from(content: internal_api::Content) -> Self {
        let features = content.features.into_iter().map(|f| f.into()).collect();
        Self {
            content_type: content.mime,
            bytes: content.bytes,
            features,
            labels: content.labels,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractRequest {
    pub name: String,
    pub content: Content,
    pub input_params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractResponse {
    pub content: Vec<Content>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum IngestExtractedContent {
    BeginExtractedContentIngest(BeginExtractedContentIngest),
    ExtractedFeatures(ExtractedFeatures),
    ExtractedContent(ExtractedContent),
    FinishExtractedContentIngest(FinishExtractedContentIngest),
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct IngestExtractedContentResponse {}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct BeginExtractedContentIngest {
    pub task_id: String,
    pub namespace: String,
    pub output_to_index_table_mapping: HashMap<String, String>,
    pub parent_content_id: String,
    pub executor_id: String,
    pub task_outcome: internal_api::TaskOutcome,
    pub extraction_policy: String,
    pub extractor: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ExtractedContent {
    pub content_list: Vec<internal_api::Content>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ExtractedFeatures {
    pub content_id: String,
    pub features: Vec<Feature>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FinishExtractedContentIngest {
    pub num_extracted_content: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetContentMetadataResponse {
    pub content_metadata: ContentMetadata,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetExtractedMetadataResponse {
    pub extracted_metadata: Vec<ExtractedMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasks {
    pub extraction_policy: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListTasksResponse {
    pub tasks: Vec<internal_api::Task>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListStateChanges {
    pub start_at: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListStateChangesResponse {
    pub state_changes: Vec<internal_api::StateChange>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetStructuredDataSchemasResponse {
    pub schemas: Vec<StructuredDataSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StructuredDataSchema {
    pub columns: serde_json::Value,
    pub content_source: String,
    pub namespace: String,
}

#[derive(Serialize, Deserialize)]
pub struct RaftMetricsSnapshotResponse {
    pub fail_connect_to_peer: HashMap<String, u64>,
    pub sent_bytes: HashMap<String, u64>,
    pub recv_bytes: HashMap<String, u64>,
    pub sent_failures: HashMap<String, u64>,
    pub snapshot_send_success: HashMap<String, u64>,
    pub snapshot_send_failure: HashMap<String, u64>,
    pub snapshot_recv_success: HashMap<String, u64>,
    pub snapshot_recv_failure: HashMap<String, u64>,
    pub snapshot_send_inflights: HashMap<String, u64>,
    pub snapshot_recv_inflights: HashMap<String, u64>,
    pub snapshot_sent_seconds: HashMap<String, Vec<u64>>,
    pub snapshot_recv_seconds: HashMap<String, Vec<u64>>,
    pub snapshot_size: Vec<u64>,
    pub last_snapshot_creation_time_millis: u64,
    pub running_state_ok: bool,
    pub id: u64,
    pub current_term: u64,
    pub vote: u64,
    pub last_log_index: u64,
    pub current_leader: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SQLQuery {
    pub query: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlQueryResponse {
    pub rows: Vec<serde_json::Value>,
}
