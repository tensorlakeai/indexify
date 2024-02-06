mod api_utils;

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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractorBinding {
    pub extractor: String,
    pub name: String,
    #[serde(default, deserialize_with = "api_utils::deserialize_labels_eq_filter")]
    pub filters_eq: Option<HashMap<String, String>>,
    pub input_params: Option<serde_json::Value>,
    pub content_source: Option<String>,
}

impl From<ExtractorBinding> for indexify_coordinator::ExtractorBinding {
    fn from(value: ExtractorBinding) -> Self {
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
pub struct DataRepository {
    pub name: String,
    pub extractor_bindings: Vec<ExtractorBinding>,
}

impl TryFrom<indexify_coordinator::Repository> for DataRepository {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Repository) -> Result<Self> {
        let mut extractor_bindings = Vec::new();
        for binding in value.bindings {
            extractor_bindings.push(ExtractorBinding {
                extractor: binding.extractor,
                name: binding.name,
                filters_eq: Some(binding.filters),
                input_params: Some(serde_json::from_str(&binding.input_params)?),
                content_source: Some(binding.content_source),
            });
        }
        Ok(Self {
            name: value.name,
            extractor_bindings,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct CreateRepository {
    pub name: String,
    pub extractor_bindings: Vec<ExtractorBinding>,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateRepositoryResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepositoryResponse {
    pub repository: DataRepository,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListRepositoriesResponse {
    pub repositories: Vec<DataRepository>,
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

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExtractorBindRequest {
    #[serde(flatten)]
    pub extractor_binding: ExtractorBinding,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ExtractorBindResponse {
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
#[serde(untagged)]
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

impl From<ExtractorDescription> for indexify_coordinator::Extractor {
    fn from(value: ExtractorDescription) -> Self {
        let outputs = value
            .outputs
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();
        Self {
            name: value.name,
            description: value.description,
            input_params: value.input_params.to_string(),
            outputs,
            input_mime_types: value.input_mime_types,
        }
    }
}

impl From<ExtractorDescription> for internal_api::ExtractorDescription {
    fn from(extractor: ExtractorDescription) -> internal_api::ExtractorDescription {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.outputs {
            match embedding_schema {
                ExtractorOutputSchema::Embedding(embedding_schema) => {
                    let distance = embedding_schema.distance.to_string();
                    output_schema.insert(
                        output_name,
                        internal_api::OutputSchema::Embedding(internal_api::EmbeddingSchema {
                            dim: embedding_schema.dim,
                            distance,
                        }),
                    );
                }
                ExtractorOutputSchema::Metadata(schema) => {
                    output_schema
                        .insert(output_name, internal_api::OutputSchema::Attributes(schema));
                }
            }
        }
        Self {
            name: extractor.name,
            description: extractor.description,
            input_params: extractor.input_params,
            outputs: output_schema,
            input_mime_types: extractor.input_mime_types,
        }
    }
}

impl TryFrom<indexify_coordinator::Extractor> for ExtractorDescription {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Extractor) -> Result<Self> {
        let mut outputs = HashMap::new();
        for (k, v) in value.outputs.iter() {
            let v: ExtractorOutputSchema = serde_json::from_str(v)?;
            outputs.insert(k.clone(), v);
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
    pub content_id: Option<String>,
    pub index: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct MetadataResponse {
    pub attributes: Vec<ExtractedMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct DocumentFragment {
    pub content_id: String,
    pub text: String,
    pub confidence_score: f32,
    pub labels: HashMap<String, String>,
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

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ListContentResponse {
    pub content_list: Vec<ContentMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ContentMetadata {
    pub id: String,
    pub parent_id: String,
    pub repository: String,
    pub name: String,
    pub content_type: String,
    pub labels: HashMap<String, String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString, ToSchema)]
pub enum FeatureType {
    #[strum(serialize = "embedding")]
    Embedding,
    #[strum(serialize = "metadata")]
    Metadata,
    #[strum(serialize = "unknown")]
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct WriteExtractedContent {
    pub content_list: Vec<internal_api::Content>,
    pub task_id: String,
    pub repository: String,
    pub output_to_index_table_mapping: HashMap<String, String>,
    pub parent_content_id: String,
    pub executor_id: String,
    pub task_outcome: internal_api::TaskOutcome,
    pub extractor_binding: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GetRawContentResponse {
    pub content_list: Vec<Content>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasks {
    pub repository: String,
    pub extractor_binding: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
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
