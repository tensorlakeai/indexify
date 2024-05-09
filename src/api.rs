use std::collections::HashMap;

use anyhow::{anyhow, Result};
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
pub struct ExtractionGraph {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl From<ExtractionGraph> for indexify_coordinator::ExtractionGraph {
    fn from(value: ExtractionGraph) -> Self {
        Self {
            id: value.id,
            namespace: value.namespace.clone(),
            name: value.name,
            extraction_policies: value
                .extraction_policies
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<indexify_coordinator::ExtractionGraph> for ExtractionGraph {
    fn from(value: indexify_coordinator::ExtractionGraph) -> Self {
        Self {
            id: value.namespace.clone(),
            namespace: value.namespace,
            name: value.name,
            extraction_policies: value
                .extraction_policies
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractionPolicy {
    pub id: String,
    pub extractor: String,
    pub name: String,
    #[serde(default, deserialize_with = "api_utils::deserialize_labels_eq_filter")]
    pub filters_eq: Option<HashMap<String, String>>,
    pub input_params: Option<serde_json::Value>,
    pub content_source: Option<String>,
    pub graph_name: String,
}

impl From<ExtractionPolicy> for indexify_coordinator::ExtractionPolicy {
    fn from(value: ExtractionPolicy) -> Self {
        Self {
            id: value.id,
            extractor: value.extractor,
            name: value.name,
            filters: value.filters_eq.unwrap_or_default(),
            input_params: value
                .input_params
                .map(|v| v.to_string())
                .unwrap_or("{}".to_string()),
            content_source: value.content_source.unwrap_or("ingestion".to_string()),
            graph_name: value.graph_name,
        }
    }
}

impl From<indexify_coordinator::ExtractionPolicy> for ExtractionPolicy {
    fn from(value: indexify_coordinator::ExtractionPolicy) -> Self {
        Self {
            id: value.id,
            extractor: value.extractor,
            name: value.name,
            filters_eq: Some(value.filters),
            input_params: Some(serde_json::from_str(&value.input_params).unwrap()),
            content_source: Some(value.content_source),
            graph_name: value.graph_name,
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataNamespace {
    pub name: String,
    pub extraction_graphs: Vec<ExtractionGraph>,
}

impl From<indexify_coordinator::Namespace> for DataNamespace {
    fn from(value: indexify_coordinator::Namespace) -> Self {
        Self {
            name: value.name,
            extraction_graphs: value
                .extraction_graphs
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct CreateNamespace {
    pub name: String,
    pub extraction_graphs: Vec<ExtractionGraph>,
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
    pub extractor: String,
    pub name: String,
    #[serde(default, deserialize_with = "api_utils::deserialize_labels_eq_filter")]
    pub filters_eq: Option<HashMap<String, String>>,
    pub input_params: Option<serde_json::Value>,
    pub content_source: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ExtractionPolicyResponse {
    #[serde(default)]
    pub index_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Text {
    pub id: Option<String>,
    pub text: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TextAddRequest {
    pub documents: Vec<Text>,
    pub sync: Option<bool>,
    pub extraction_graph_names: Vec<internal_api::ExtractionGraphName>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateContentRequest {
    pub content_id: String,
    pub content: Vec<Text>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateContentResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TombstoneContentRequest {
    pub content_ids: Vec<String>,
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
            input_params: serde_json::from_str(&value.input_params)?,
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
    pub embedding_schema: EmbeddingSchema,
}

impl TryFrom<indexify_coordinator::Index> for Index {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Index) -> Result<Self> {
        Ok(Self {
            name: value.name,
            embedding_schema: serde_json::from_str(&value.schema).map_err(|e| {
                anyhow!(
                    "unable to create embedding schema from: {}, error: {}",
                    value.schema,
                    e.to_string()
                )
            })?,
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
    #[serde(default)]
    pub filters: Vec<String>,
    pub include_content: Option<bool>,
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
    pub root_content_metadata: Option<ContentMetadata>,
    pub content_metadata: ContentMetadata,
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
    pub fn new(status_code: StatusCode, message: &str) -> Self {
        Self {
            status_code,
            message: message.to_string(),
        }
    }

    pub fn internal_error(e: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string().as_str())
    }

    pub fn not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
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
    pub root_content_id: String,
    pub namespace: String,
    pub name: String,
    pub mime_type: String,
    pub labels: HashMap<String, String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: String,
    pub size: u64,
    pub hash: String,
}

impl From<indexify_coordinator::ContentMetadata> for ContentMetadata {
    fn from(value: indexify_coordinator::ContentMetadata) -> Self {
        Self {
            id: value.id,
            parent_id: value.parent_id,
            root_content_id: value.root_content_id,
            namespace: value.namespace,
            name: value.file_name,
            mime_type: value.mime,
            labels: value.labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            source: value.source,
            size: value.size_bytes,
            hash: value.hash,
        }
    }
}

impl From<indexify_internal_api::ContentMetadata> for ContentMetadata {
    fn from(value: indexify_internal_api::ContentMetadata) -> Self {
        Self {
            id: value.id.id,
            parent_id: value.parent_id.map(|id| id.id).unwrap_or_default(),
            root_content_id: value.root_content_id.unwrap_or_default(),
            namespace: value.namespace,
            name: value.name,
            mime_type: value.content_type,
            labels: value.labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            source: value.source.to_string(),
            size: value.size_bytes,
            hash: value.hash,
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, ToSchema)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extraction_policy_id: String,
    pub output_index_table_mapping: HashMap<String, String>,
    pub namespace: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
    pub outcome: i32,
    pub index_tables: Vec<String>,
}

impl From<indexify_coordinator::Task> for Task {
    fn from(value: indexify_coordinator::Task) -> Self {
        Self {
            id: value.id,
            extractor: value.extractor,
            extraction_policy_id: value.extraction_policy_id,
            output_index_table_mapping: value.output_index_mapping,
            namespace: value.namespace,
            content_metadata: value
                .content_metadata
                .map_or_else(Default::default, Into::into), //  EGTODO: Is this correct?
            input_params: serde_json::Value::String(value.input_params),
            outcome: value.outcome, //  EGTODO: Is it correct to just return i32 for value outcome?
            index_tables: value.index_tables,
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
pub struct ContentWithId {
    pub id: String,
    pub content: Content,
    pub extraction_graph_names: Vec<internal_api::ExtractionGraphName>,
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
            content_type: content.content_type,
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
    pub features: Vec<Feature>,
}

impl From<internal_api::ExtractResponse> for ExtractResponse {
    fn from(internal_resp: internal_api::ExtractResponse) -> Self {
        ExtractResponse {
            content: internal_resp
                .content
                .into_iter()
                .map(Content::from)
                .collect(),
            features: internal_resp
                .features
                .into_iter()
                .map(Feature::from)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContentFrame {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FinishContent {
    pub content_type: String,
    pub features: Vec<Feature>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BeginMultiPartContent {
    pub id: i32,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum IngestExtractedContent {
    BeginExtractedContentIngest(BeginExtractedContentIngest),
    ExtractedFeatures(ExtractedFeatures),
    FinishExtractedContentIngest(FinishExtractedContentIngest),
    BeginMultipartContent(BeginMultiPartContent),
    MultipartContentFrame(ContentFrame),
    FinishMultipartContent(FinishContent),
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum IngestExtractedContentResponse {
    Success,
    Error(String),
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct BeginExtractedContentIngest {
    pub task_id: String,
    pub executor_id: String,
    pub task_outcome: internal_api::TaskOutcome,
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
pub struct GetContentTreeMetadataResponse {
    pub content_tree_metadata: Vec<ContentMetadata>,
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
    pub tasks: Vec<Task>,
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
    pub schemas: Vec<internal_api::StructuredDataSchema>,
    pub ddls: HashMap<String, String>,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestRemoteFile {
    pub id: Option<String>,
    pub url: String,
    pub mime_type: String,
    pub labels: HashMap<String, String>,
    pub extraction_graph_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestRemoteFileResponse {
    pub content_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskAssignments {
    pub assignments: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadFileResponse {
    pub content_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractionGraphRequest {
    pub name: String,
    pub namespace: String,
    pub policies: Vec<ExtractionPolicyRequest>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractionGraphResponse {
    pub indexes: Vec<String>,
}
