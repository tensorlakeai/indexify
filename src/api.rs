use std::collections::HashMap;

use anyhow::{anyhow, Result};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use filter::{Expression, LabelsFilter};
use indexify_internal_api::{self as internal_api, ContentOffset};
use indexify_proto::indexify_coordinator::{self};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use strum::{Display, EnumString};
use utoipa::{openapi, IntoParams, ToSchema};

use crate::{api_utils, metadata_storage, vectordbs};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractionGraphLink {
    pub content_source: String,
    pub linked_graph_name: String,
}

impl From<indexify_coordinator::ExtractionGraphLink> for ExtractionGraphLink {
    fn from(value: indexify_coordinator::ExtractionGraphLink) -> Self {
        Self {
            content_source: value.content_source,
            linked_graph_name: value.linked_graph_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractionGraph {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    pub description: Option<String>,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl TryFrom<indexify_coordinator::ExtractionGraph> for ExtractionGraph {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ExtractionGraph) -> Result<Self> {
        let mut extraction_policies = vec![];
        for policy in value.extraction_policies {
            let policy: ExtractionPolicy = policy.try_into()?;
            extraction_policies.push(policy);
        }

        let description = if value.description.is_empty() {
            None
        } else {
            Some(value.description)
        };
        Ok(Self {
            id: value.id,
            namespace: value.namespace,
            name: value.name,
            description,
            extraction_policies,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractionPolicy {
    pub id: String,
    pub extractor: String,
    pub name: String,
    #[serde(default)]
    #[schema(schema_with = filter_schema)]
    pub filter: filter::LabelsFilter,
    pub input_params: Option<serde_json::Value>,
    pub content_source: Option<String>,
    pub graph_name: String,
}

impl TryFrom<indexify_coordinator::ExtractionPolicy> for ExtractionPolicy {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ExtractionPolicy) -> Result<Self> {
        let expressions: Result<Vec<_>> = value
            .filter
            .iter()
            .map(|e| filter::Expression::from_str(e))
            .collect();
        Ok(Self {
            id: value.id,
            extractor: value.extractor,
            name: value.name,
            filter: LabelsFilter(expressions?),
            input_params: Some(serde_json::from_str(&value.input_params).unwrap()),
            content_source: Some(value.content_source),
            graph_name: value.graph_name,
        })
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataNamespace {
    pub name: String,
}

impl TryFrom<indexify_coordinator::Namespace> for DataNamespace {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Namespace) -> Result<Self> {
        Ok(Self { name: value.name })
    }
}

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

fn filter_schema() -> openapi::Array {
    openapi::ArrayBuilder::new()
        .description(Some("Filter for content labels, list of expressions."))
        .items(
            openapi::ObjectBuilder::new()
                .schema_type(openapi::SchemaType::String)
                .description(Some(
                    "filter expression in format key/operator/value, e.g. key>=value",
                ))
                .build(),
        )
        .example(Some(serde_json::json!(vec!["key1=value1", "key2>=value2"])))
        .build()
}

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExtractionPolicyRequest {
    pub extractor: String,
    pub name: String,
    #[serde(default)]
    #[schema(schema_with = filter_schema)]
    pub filter: LabelsFilter,
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
    pub labels: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TextAddRequest {
    pub documents: Vec<Text>,
    pub sync: Option<bool>,

    // internal_api::ExtractionGraphName, can't use it here because ToSchema is not implemented
    pub extraction_graph_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateContentRequest {
    pub content_id: String,
    pub content: Vec<Text>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateContentResponse {}

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
pub struct TextAdditionResponse {
    pub content_ids: Vec<String>,
}

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
    pub query: String,
    pub k: Option<u64>,
    #[serde(default)]
    #[schema(schema_with = filter_schema)]
    pub filters: LabelsFilter,
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListExtractionGraphResponse {
    pub extraction_graphs: Vec<ExtractionGraph>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Clone)]
pub struct ListContent {
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
    #[serde(
        deserialize_with = "api_utils::deserialize_none_to_empty_string",
        default
    )]
    pub ingested_content_id: String,
    #[serde(default)]
    pub labels_filter: Vec<Expression>,

    pub limit: Option<u64>,

    pub start_id: Option<String>,
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
    pub labels: HashMap<String, serde_json::Value>,
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
    pub total: u64,
}

impl TryFrom<indexify_proto::indexify_coordinator::ListContentResponse> for ListContentResponse {
    type Error = anyhow::Error;

    fn try_from(value: indexify_proto::indexify_coordinator::ListContentResponse) -> Result<Self> {
        let content_list = value
            .content_list
            .into_iter()
            .map(|content| content.try_into())
            .collect::<Result<Vec<ContentMetadata>>>()?;

        Ok(Self {
            content_list,
            total: value.total,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema, IntoParams)]
pub struct UpdateLabelsRequest {
    pub labels: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema, Clone)]
pub struct ContentMetadata {
    pub id: String,
    pub parent_id: String,
    pub root_content_id: String,
    pub namespace: String,
    pub name: String,
    pub mime_type: String,
    pub labels: HashMap<String, serde_json::Value>,
    pub extraction_graph_names: Vec<String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: String,
    pub size: u64,
    pub hash: String,
    pub extracted_metadata: serde_json::Value,
}

impl TryFrom<indexify_coordinator::ContentMetadata> for ContentMetadata {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ContentMetadata) -> Result<Self> {
        let labels = internal_api::utils::convert_map_prost_to_serde_json(value.labels)?;

        Ok(Self {
            id: value.id,
            parent_id: value.parent_id,
            root_content_id: value.root_content_id,
            namespace: value.namespace,
            name: value.file_name,
            mime_type: value.mime,
            labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            source: value.source,
            size: value.size_bytes,
            hash: value.hash,
            extraction_graph_names: value.extraction_graph_names,
            extracted_metadata: serde_json::from_str(&value.extracted_metadata)?,
        })
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
            extraction_graph_names: value.extraction_graph_names,
            extracted_metadata: value.extracted_metadata,
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

impl TryFrom<indexify_coordinator::Task> for Task {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Task) -> Result<Self> {
        let content_metadata = if let Some(metadata) = value.content_metadata {
            metadata.try_into()?
        } else {
            ContentMetadata::default()
        };

        Ok(Self {
            id: value.id,
            extractor: value.extractor,
            extraction_policy_id: value.extraction_policy_id,
            output_index_table_mapping: value.output_index_mapping,
            namespace: value.namespace,
            content_metadata,
            input_params: serde_json::Value::String(value.input_params),
            outcome: value.outcome, //  EGTODO: Is it correct to just return i32 for value outcome?
            index_tables: value.index_tables,
        })
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

    // internal_api::ExtractionGraphName, can't use it here because ToSchema is not implemented
    pub extraction_graph_names: Vec<String>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Content {
    pub content_type: String,
    #[serde_as(as = "BytesOrString")]
    pub bytes: Vec<u8>,
    pub features: Vec<Feature>,
    pub labels: HashMap<String, serde_json::Value>,
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum NewContentStreamStart {
    /// Last offset seen by the caller, start from next
    #[serde(rename = "from_offset")]
    FromOffset(ContentOffset),
    /// Start from the next created content
    #[serde(rename = "from_last")]
    FromLast,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NewContentStreamResponse {
    /// New content metadata
    pub content: ContentMetadata,
    /// Restart offset for the next request
    pub offset: u64,
}

impl TryFrom<indexify_proto::indexify_coordinator::ContentStreamItem> for NewContentStreamResponse {
    type Error = anyhow::Error;

    fn try_from(value: indexify_proto::indexify_coordinator::ContentStreamItem) -> Result<Self> {
        Ok(Self {
            content: value.content.unwrap().try_into()?,
            offset: value.change_offset,
        })
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
    pub labels: HashMap<String, serde_json::Value>,
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListTasks {
    pub content_id: Option<String>,
    pub start_id: Option<String>,
    pub limit: Option<u64>,
    #[serde(default)]
    pub outcome: internal_api::TaskOutcomeFilter,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TaskAnalytics {
    pub pending: u64,
    pub success: u64,
    pub failure: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractionGraphAnalytics {
    pub task_analytics: HashMap<String, TaskAnalytics>,
}

impl From<indexify_coordinator::GetExtractionGraphAnalyticsResponse> for ExtractionGraphAnalytics {
    fn from(value: indexify_coordinator::GetExtractionGraphAnalyticsResponse) -> Self {
        let mut task_analytics = HashMap::new();
        for (k, v) in value.task_analytics.iter() {
            task_analytics.insert(
                k.clone(),
                TaskAnalytics {
                    pending: v.pending,
                    success: v.success,
                    failure: v.failure,
                },
            );
        }
        Self { task_analytics }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListTasksResponse {
    pub tasks: Vec<Task>,
    pub total: u64,
}

impl TryFrom<indexify_proto::indexify_coordinator::ListTasksResponse> for ListTasksResponse {
    type Error = IndexifyAPIError;

    fn try_from(
        value: indexify_proto::indexify_coordinator::ListTasksResponse,
    ) -> Result<Self, Self::Error> {
        let tasks = value
            .tasks
            .into_iter()
            .map(|task| task.try_into().map_err(IndexifyAPIError::internal_error))
            .collect::<Result<Vec<Task>, Self::Error>>()?;

        Ok(Self {
            tasks,
            total: value.total,
        })
    }
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct IngestRemoteFile {
    pub id: Option<String>,
    pub url: String,
    pub mime_type: String,
    #[serde(default)]
    pub labels: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct IngestRemoteFileResponse {
    pub content_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskAssignments {
    pub assignments: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UploadFileResponse {
    pub content_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AddGraphToContent {
    /// List of existing content ids to extract from
    pub content_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractionGraphRequest {
    pub name: String,
    pub description: Option<String>,
    pub extraction_policies: Vec<ExtractionPolicyRequest>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractionGraphResponse {
    pub indexes: Vec<String>,
}
