use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use anyhow::{anyhow, Result};
use indexify_proto::indexify_coordinator;
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};
use utoipa::{schema, ToSchema};

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize, Default)]
pub struct Index {
    // TODO FIXME: Add the Index ID
    pub namespace: String,
    pub name: String,
    pub table_name: String,
    pub schema: String,
    pub extraction_policy: String,
    pub extractor: String,
}

impl Index {
    pub fn id(&self) -> String {
        let mut s = DefaultHasher::new();
        self.namespace.hash(&mut s);
        self.name.hash(&mut s);
        format!("{:x}", s.finish())
    }
}

impl Hash for Index {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.name.hash(state);
    }
}

impl From<Index> for indexify_coordinator::Index {
    fn from(value: Index) -> Self {
        Self {
            name: value.name,
            table_name: value.table_name,
            schema: value.schema,
            extractor: value.extractor,
            extraction_policy: value.extraction_policy,
            namespace: value.namespace,
        }
    }
}

impl From<indexify_coordinator::Index> for Index {
    fn from(value: indexify_coordinator::Index) -> Self {
        Self {
            name: value.name,
            table_name: value.table_name,
            schema: value.schema,
            extractor: value.extractor,
            extraction_policy: value.extraction_policy,
            namespace: value.namespace,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingSchema {
    pub dim: usize,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Embedding {
    pub values: Vec<f32>,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OutputSchema {
    #[serde(rename = "embedding")]
    Embedding(EmbeddingSchema),
    #[serde(rename = "attributes")]
    Attributes(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractorDescription {
    pub name: String,
    pub description: String,
    pub input_params: serde_json::Value,
    pub outputs: HashMap<String, OutputSchema>,
    pub input_mime_types: Vec<String>,
}

impl From<ExtractorDescription> for indexify_coordinator::Extractor {
    fn from(value: ExtractorDescription) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in value.outputs {
            output_schema.insert(
                output_name,
                serde_json::to_string(&embedding_schema).unwrap(),
            );
        }
        Self {
            name: value.name,
            description: value.description,
            input_params: value.input_params.to_string(),
            outputs: output_schema,
            input_mime_types: value.input_mime_types,
        }
    }
}

impl From<indexify_coordinator::Extractor> for ExtractorDescription {
    fn from(value: indexify_coordinator::Extractor) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in value.outputs {
            let embedding_schema: OutputSchema = serde_json::from_str(&embedding_schema).unwrap();
            output_schema.insert(output_name, embedding_schema);
        }
        Self {
            name: value.name,
            description: value.description,
            input_params: serde_json::from_str(&value.input_params).unwrap(),
            outputs: output_schema,
            input_mime_types: value.input_mime_types,
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

#[derive(
    Debug, PartialEq, Eq, Serialize, Clone, Deserialize, EnumString, Display, SmartDefault,
)]
pub enum TaskState {
    #[default]
    Unknown,
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CreateWork {
    pub namespace: String,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateWorkResponse {}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString)]
pub enum FeatureType {
    #[strum(serialize = "embedding")]
    #[serde(rename = "embedding")]
    Embedding,
    #[strum(serialize = "metadata")]
    #[serde(rename = "metadata")]
    Metadata,
    #[strum(serialize = "unknown")]
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    pub feature_type: FeatureType,
    pub name: String,
    pub data: serde_json::Value,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
// Specifying alternate name as utoipa has issues with namespaced schemas. See https://github.com/juhaku/utoipa/issues/569#issuecomment-1503466468
#[schema(as = internal_api::Content)]
pub struct Content {
    pub mime: String,
    #[serde_as(as = "BytesOrString")]
    pub bytes: Vec<u8>,
    pub features: Vec<Feature>,
    pub labels: HashMap<String, String>,
}

impl Content {
    pub fn source_as_text(&self) -> Option<String> {
        let mime_type = mime::Mime::from_str(&self.mime);
        if let Ok(mime_type) = mime_type {
            if mime_type == mime::TEXT_PLAIN {
                return Some(String::from_utf8(self.bytes.clone()).unwrap());
            }
        }
        None
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq, ToSchema, Default)]
#[schema(as = internal_api::TaskOutcome)]
pub enum TaskOutcome {
    Unknown,
    #[default]
    Success,
    Failed,
}

impl From<indexify_coordinator::TaskOutcome> for TaskOutcome {
    fn from(value: indexify_coordinator::TaskOutcome) -> Self {
        match value {
            indexify_coordinator::TaskOutcome::Unknown => TaskOutcome::Unknown,
            indexify_coordinator::TaskOutcome::Success => TaskOutcome::Success,
            indexify_coordinator::TaskOutcome::Failed => TaskOutcome::Failed,
        }
    }
}

impl From<TaskOutcome> for indexify_coordinator::TaskOutcome {
    fn from(value: TaskOutcome) -> Self {
        match value {
            TaskOutcome::Unknown => indexify_coordinator::TaskOutcome::Unknown,
            TaskOutcome::Success => indexify_coordinator::TaskOutcome::Success,
            TaskOutcome::Failed => indexify_coordinator::TaskOutcome::Failed,
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq, ToSchema, Default)]
#[schema(as = internal_api::Task)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extraction_policy: String,
    pub output_index_table_mapping: HashMap<String, String>,
    pub namespace: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
    #[schema(value_type = internal_api::TaskOutcome)]
    pub outcome: TaskOutcome,
}

impl From<Task> for indexify_coordinator::Task {
    fn from(value: Task) -> Self {
        let outcome: indexify_coordinator::TaskOutcome = value.outcome.into();
        Self {
            id: value.id,
            extractor: value.extractor,
            namespace: value.namespace,
            content_metadata: Some(value.content_metadata.into()),
            input_params: value.input_params.to_string(),
            extraction_policy: value.extraction_policy,
            output_index_mapping: value.output_index_table_mapping,
            outcome: outcome as i32,
        }
    }
}

impl TryFrom<indexify_coordinator::Task> for Task {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Task) -> Result<Self> {
        let content_metadata: ContentMetadata = value.content_metadata.unwrap().try_into()?;
        let outcome: TaskOutcome =
            indexify_coordinator::TaskOutcome::try_from(value.outcome)?.into();
        Ok(Self {
            id: value.id,
            extractor: value.extractor,
            namespace: value.namespace,
            content_metadata,
            input_params: serde_json::from_str(&value.input_params).unwrap(),
            extraction_policy: value.extraction_policy,
            output_index_table_mapping: value.output_index_mapping,
            outcome,
        })
    }
}
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize)]
pub struct ExtractionPolicy {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub extractor: String,
    pub filters: HashMap<String, String>,
    pub input_params: serde_json::Value,

    // Output name of the extractor to index name where the
    // ouput is written to
    pub output_index_name_mapping: HashMap<String, String>,

    // Index name to the underlying table name of the index
    // in storage system
    pub index_name_table_mapping: HashMap<String, String>,

    // The source of the content - ingestion, name of some extractor binding
    // which produces the content by invoking an extractor
    pub content_source: String,
}

impl std::hash::Hash for ExtractionPolicy {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.name.hash(state);
    }
}

impl From<ExtractionPolicy> for indexify_coordinator::ExtractionPolicy {
    fn from(value: ExtractionPolicy) -> Self {
        let mut filters = HashMap::new();
        for filter in value.filters {
            filters.insert(filter.0, filter.1.to_string());
        }

        Self {
            extractor: value.extractor,
            name: value.name,
            filters,
            input_params: value.input_params.to_string(),
            content_source: value.content_source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ContentMetadata {
    pub id: String,
    pub parent_id: String,
    // Namespace name == Namespace ID
    pub namespace: String,
    pub name: String,
    pub content_type: String,
    pub labels: HashMap<String, String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: String,
    pub size_bytes: u64,
}

impl From<ContentMetadata> for indexify_coordinator::ContentMetadata {
    fn from(value: ContentMetadata) -> Self {
        Self {
            id: value.id,
            parent_id: value.parent_id,
            file_name: value.name,
            mime: value.content_type,
            labels: value.labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            namespace: value.namespace,
            source: value.source,
            size_bytes: value.size_bytes,
        }
    }
}

impl Default for ContentMetadata {
    fn default() -> Self {
        Self {
            id: "test_id".to_string(),
            parent_id: "test_parent_id".to_string(),
            namespace: "test_namespace".to_string(),
            name: "test_name".to_string(),
            content_type: "test_content_type".to_string(),
            labels: {
                let mut labels = HashMap::new();
                labels.insert("key1".to_string(), "value1".to_string());
                labels.insert("key2".to_string(), "value2".to_string());
                labels
            },
            storage_url: "http://example.com/test_url".to_string(),
            created_at: 1234567890, // example timestamp
            source: "test_source".to_string(),
            size_bytes: 1234567890,
        }
    }
}

impl TryFrom<indexify_coordinator::ContentMetadata> for ContentMetadata {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ContentMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            parent_id: value.parent_id,
            name: value.file_name,
            content_type: value.mime,
            labels: value.labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            namespace: value.namespace,
            source: value.source,
            size_bytes: value.size_bytes,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
pub struct TaskResult {
    pub task_id: String,
    pub outcome: TaskOutcome,
    pub extracted_content: Vec<Content>,
    pub error_msg: Option<String>,
}

impl TaskResult {
    pub fn failed(task_id: &str, msg: Option<String>) -> Self {
        Self {
            task_id: task_id.to_string(),
            outcome: TaskOutcome::Failed,
            extracted_content: Vec::new(),
            error_msg: msg,
        }
    }

    pub fn success(task_id: &str, extracted_content: Vec<Content>) -> Self {
        Self {
            task_id: task_id.to_string(),
            outcome: TaskOutcome::Success,
            extracted_content,
            error_msg: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Namespace {
    pub name: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl From<Namespace> for indexify_coordinator::Namespace {
    fn from(value: Namespace) -> Self {
        Self {
            name: value.name,
            policies: value
                .extraction_policies
                .into_iter()
                .map(|b| b.into())
                .collect(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ChangeType {
    NewContent,
    NewExtractionPolicy,
    ExecutorAdded,
    ExecutorRemoved,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::NewContent => write!(f, "NewContent"),
            ChangeType::NewExtractionPolicy => write!(f, "NewBinding"),
            ChangeType::ExecutorAdded => write!(f, "ExecutorAdded"),
            ChangeType::ExecutorRemoved => write!(f, "ExecutorRemoved"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StateChange {
    pub id: String,
    pub object_id: String,
    pub change_type: ChangeType,
    pub created_at: u64,
    pub processed_at: Option<u64>,
}

impl Default for StateChange {
    fn default() -> Self {
        Self {
            id: "".to_string(),
            object_id: "".to_string(),
            change_type: ChangeType::NewContent,
            created_at: 0,
            processed_at: None,
        }
    }
}

impl StateChange {
    pub fn new(object_id: String, change_type: ChangeType, created_at: u64) -> Self {
        Self {
            id: nanoid!(16),
            object_id,
            change_type,
            created_at,
            processed_at: None,
        }
    }
}

impl TryFrom<indexify_coordinator::StateChange> for StateChange {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::StateChange) -> Result<Self> {
        let change_type = match value.change_type.as_str() {
            "NewContent" => ChangeType::NewContent,
            "NewBinding" => ChangeType::NewExtractionPolicy,
            "ExecutorAdded" => ChangeType::ExecutorAdded,
            "ExecutorRemoved" => ChangeType::ExecutorRemoved,
            _ => return Err(anyhow!("Invalid ChangeType")),
        };
        Ok(Self {
            id: value.id,
            object_id: value.object_id,
            change_type,
            created_at: value.created_at,
            processed_at: Some(value.processed_at),
        })
    }
}

impl From<StateChange> for indexify_coordinator::StateChange {
    fn from(value: StateChange) -> Self {
        Self {
            id: value.id,
            object_id: value.object_id,
            change_type: value.change_type.to_string(),
            created_at: value.created_at,
            processed_at: value.processed_at.unwrap_or(0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub embedding: Vec<f32>,
}
