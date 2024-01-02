use std::{collections::HashMap, str::FromStr, hash::Hasher};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};

use crate::{
    api,
    indexify_coordinator,
    persistence::{self},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputSchema {
    Embedding { dim: usize, distance: String },
    Feature(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub output: HashMap<String, OutputSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorDescription {
    pub name: String,
    pub description: String,
    pub input_params: serde_json::Value,
    pub schema: ExtractorSchema,
}

impl From<ExtractorDescription> for indexify_coordinator::Extractor {
    fn from(value: ExtractorDescription) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in value.schema.output {
            output_schema.insert(output_name, serde_json::to_string(&embedding_schema).unwrap());
        }
        Self {
            name: value.name,
            description: value.description,
            input_params: value.input_params.to_string(),
            outputs: output_schema,
        }
    }
}

impl From<api::ExtractorDescription> for ExtractorDescription {
    fn from(extractor: api::ExtractorDescription) -> ExtractorDescription {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.schemas.outputs {
            match embedding_schema {
                api::ExtractorOutputSchema::Embedding { dim, distance } => {
                    let distance_metric = distance.to_string();
                    output_schema.insert(
                        output_name,
                        OutputSchema::Embedding {
                            dim,
                            distance: distance_metric,
                        },
                    );
                }
                api::ExtractorOutputSchema::Attributes { schema } => {
                    output_schema.insert(output_name, OutputSchema::Feature(schema));
                }
            }
        }
        Self {
            name: extractor.name,
            description: extractor.description,
            input_params: extractor.input_params,
            schema: ExtractorSchema {
                output: output_schema,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorInfo {
    pub id: String,
    pub last_seen: u64,
    pub addr: String,
    pub extractor: ExtractorDescription,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractRequest {
    pub content: Content,
    pub input_params: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractResponse {
    pub content: Vec<Content>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CoordinateRequest {
    pub extractor_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CoordinateResponse {
    pub content: Vec<String>,
}

#[derive(
    Debug, PartialEq, Eq, Serialize, Clone, Deserialize, EnumString, Display, SmartDefault,
)]
pub enum WorkState {
    #[default]
    Unknown,
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl From<WorkState> for persistence::WorkState {
    fn from(work_state: WorkState) -> Self {
        match work_state {
            WorkState::Unknown => persistence::WorkState::Unknown,
            WorkState::Pending => persistence::WorkState::Pending,
            WorkState::InProgress => persistence::WorkState::InProgress,
            WorkState::Completed => persistence::WorkState::Completed,
            WorkState::Failed => persistence::WorkState::Failed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkStatus {
    pub work_id: String,
    pub status: WorkState,
    pub extracted_content: Vec<Content>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncExecutor {
    pub executor_id: String,
    pub extractor: ExtractorDescription,
    pub addr: String,
    pub work_status: Vec<WorkStatus>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SyncWorkerResponse {
    pub content_to_process: Vec<Work>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CreateWork {
    pub repository_name: String,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateWorkResponse {}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString)]
pub enum FeatureType {
    #[strum(serialize = "embedding")]
    Embedding,
    #[strum(serialize = "ner")]
    NamedEntity,
    #[strum(serialize = "metadata")]
    Metadata,
    #[strum(serialize = "unknown")]
    Unknown,
}

impl From<FeatureType> for api::FeatureType {
    fn from(feature_type: FeatureType) -> Self {
        match feature_type {
            FeatureType::Embedding => api::FeatureType::Embedding,
            FeatureType::NamedEntity => api::FeatureType::NamedEntity,
            FeatureType::Metadata => api::FeatureType::Metadata,
            FeatureType::Unknown => api::FeatureType::Unknown,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    pub feature_type: FeatureType,
    pub name: String,
    pub data: serde_json::Value,
}

impl Feature {
    pub fn embedding(&self) -> Option<Vec<f32>> {
        match self.feature_type {
            FeatureType::Embedding => serde_json::from_value(self.data.clone()).ok(),
            _ => None,
        }
    }

    pub fn metadata(&self) -> Option<serde_json::Value> {
        match self.feature_type {
            FeatureType::Metadata | FeatureType::NamedEntity => Some(self.data.clone()),
            _ => None,
        }
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Content {
    pub content_type: String,
    #[serde_as(as = "BytesOrString")]
    pub source: Vec<u8>,
    pub feature: Option<Feature>,
}

impl Content {
    pub fn source_as_text(&self) -> Option<String> {
        let mime_type = mime::Mime::from_str(&self.content_type);
        if let Ok(mime_type) = mime_type {
            if mime_type == mime::TEXT_PLAIN {
                return Some(String::from_utf8(self.source.clone()).unwrap());
            }
        }
        None
    }
}

impl From<Content> for api::Content {
    fn from(content: Content) -> Self {
        Self {
            content_type: content.content_type,
            source: content.source,
            feature: content.feature.map(|f| api::Feature {
                feature_type: f.feature_type.into(),
                name: f.name,
                data: f.data,
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContentPayload {
    pub content_type: String,
    pub content: String,
    pub external_url: Option<String>,
}

impl TryFrom<persistence::ContentPayload> for ContentPayload {
    type Error = anyhow::Error;

    fn try_from(payload: persistence::ContentPayload) -> Result<Self> {
        let content_type = payload.content_type.to_string();
        let (external_url, content) = match payload.payload_type {
            persistence::PayloadType::BlobStorageLink => (Some(payload.payload), "".to_string()),
            _ => (None, payload.payload),
        };
        Ok(Self {
            content_type,
            content,
            external_url,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Work {
    pub id: String,
    pub content_payload: ContentPayload,
    pub params: serde_json::Value,
}

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub repository: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
}

#[derive(Serialize, Debug, Deserialize, Display, EnumString, Clone)]
pub enum ExtractionEventPayload {
    ExtractorBindingAdded { repository: String, id: String },
    CreateContent { content_id: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractionEvent {
    pub id: String,
    pub repository_id: String,
    pub payload: ExtractionEventPayload,
    pub created_at: u64,
    pub processed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumString, Display, Eq, PartialEq)]
#[serde(rename = "extractor_filter")]
pub enum ExtractorFilter {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Neq {
        field: String,
        value: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize)]
pub struct ExtractorBinding {
    pub id: String,
    pub name: String,
    pub repository: String,
    pub extractor: String,
    pub filters: Vec<ExtractorFilter>,
    pub input_params: serde_json::Value,
}

impl std::hash::Hash for ExtractorBinding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.repository.hash(state);
        self.name.hash(state);
    }
}

impl From<ExtractorBinding> for indexify_coordinator::ExtractorBinding {
   fn from(value: ExtractorBinding) -> Self {
        let mut eq_filters = HashMap::new();
        let mut neq_filters = HashMap::new();
        for filter in value.filters {
            match filter {
                ExtractorFilter::Eq { field, value } => {
                    eq_filters.insert(field,value.to_string());
                }
                ExtractorFilter::Neq { field, value } => {
                    neq_filters.insert(field,value.to_string());
                }
            }
        }
 
        Self {
            extractor: value.extractor,
            name: value.name,
            eq_filters: eq_filters,
            neq_filters: neq_filters,
            input_params: value.input_params.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentMetadata {
    pub id: String,
    pub parent_id: String,
    pub repository: String,
    pub name: String,
    pub content_type: String,
    pub metadata: HashMap<String, serde_json::Value>,
    pub storage_url: String,
    pub created_at: i64,
}

impl From<ContentMetadata> for indexify_coordinator::ContentMetadata {
    fn from(value: ContentMetadata) -> Self {
        let mut labels = HashMap::new();
        for (key, value) in value.metadata.iter() {
            labels.insert(key.to_owned(), serde_json::to_string(value).unwrap());
        }
        Self {
            id: value.id,
            parent_id: value.parent_id,
            file_name: value.name,
            mime: value.content_type,
            labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            repository: value.repository,
        }
    }
}

impl TryFrom<indexify_coordinator::ContentMetadata> for ContentMetadata {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ContentMetadata) -> Result<Self, Self::Error> {
        let mut metadata = HashMap::new();
        for (key, value) in value.labels.iter() {
            metadata.insert(key.to_owned(), serde_json::from_str(&value)?);
        }
        Ok(Self {
            id: value.id,
            parent_id: value.parent_id,
            name: value.file_name,
            content_type: value.mime,
            metadata,
            storage_url: value.storage_url,
            created_at: value.created_at,
            repository: value.repository,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorMetadata {
    pub id: String,
    pub last_seen: u64,
    pub addr: String,
    pub extractor: ExtractorDescription,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractorHeartbeat {
    pub executor_id: String,
    pub extractor: ExtractorDescription,
    pub addr: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ExtractorHeartbeatResponse {
    pub content_to_process: Vec<Task>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WriteRequest {
    pub task_statuses: Vec<TaskStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WriteResponse {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskStatus {
    pub task_id: String,
    pub status: WorkState,
    pub extracted_content: Vec<Content>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Repository {
    pub name: String,
    pub extractor_bindings: Vec<ExtractorBinding>,
}

impl From<Repository> for indexify_coordinator::Repository {
    fn from(value: Repository) -> Self {
        Self {
            name: value.name,
            bindings: value.extractor_bindings.into_iter().map(|b| b.into()).collect(),
        }
    }
}