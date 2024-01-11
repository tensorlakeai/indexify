use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    str::FromStr,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};

use crate::{api, indexify_coordinator};

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize)]
pub struct Index {
    pub repository: String,
    pub name: String,
    pub table_name: String,
    pub schema: String,
    pub extractor_binding: String,
    pub extractor: String,
}

impl Index {
    pub fn id(&self) -> String {
        let mut s = DefaultHasher::new();
        self.repository.hash(&mut s);
        self.name.hash(&mut s);
        format!("{:x}", s.finish())
    }
}

impl Hash for Index {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.repository.hash(state);
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
            extractor_binding: value.extractor_binding,
            repository: value.repository,
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
            extractor_binding: value.extractor_binding,
            repository: value.repository,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingSchema {
    pub dim: usize,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputSchema {
    #[serde(rename = "embedding")]
    Embedding(EmbeddingSchema),
    #[serde(rename = "attributes")]
    Attributes(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorDescription {
    pub name: String,
    pub description: String,
    pub input_params: serde_json::Value,
    pub outputs: HashMap<String, OutputSchema>,
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
        }
    }
}

impl From<api::ExtractorDescription> for ExtractorDescription {
    fn from(extractor: api::ExtractorDescription) -> ExtractorDescription {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.outputs {
            match embedding_schema {
                api::ExtractorOutputSchema::Embedding(embedding_schema) => {
                    let distance_metric = embedding_schema.distance.to_string();
                    output_schema.insert(
                        output_name,
                        OutputSchema::Embedding(EmbeddingSchema {
                            dim: embedding_schema.dim,
                            distance: distance_metric,
                        }),
                    );
                }
                api::ExtractorOutputSchema::Attributes(schema) => {
                    output_schema.insert(output_name, OutputSchema::Attributes(schema));
                }
            }
        }
        Self {
            name: extractor.name,
            description: extractor.description,
            input_params: extractor.input_params,
            outputs: output_schema,
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
    pub mime: String,
    #[serde_as(as = "BytesOrString")]
    pub bytes: Vec<u8>,
    pub feature: Option<Feature>,
    pub metadata: HashMap<String, String>,
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

impl From<Content> for api::Content {
    fn from(content: Content) -> Self {
        Self {
            content_type: content.mime,
            bytes: content.bytes,
            feature: content.feature.map(|f| api::Feature {
                feature_type: f.feature_type.into(),
                name: f.name,
                data: f.data,
            }),
            metadata: content.metadata,
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extractor_binding: String,
    pub output_index_mapping: HashMap<String, String>,
    pub repository: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
}

impl From<Task> for indexify_coordinator::Task {
    fn from(value: Task) -> Self {
        Self {
            id: value.id,
            extractor: value.extractor,
            repository: value.repository,
            content_metadata: Some(value.content_metadata.into()),
            input_params: value.input_params.to_string(),
            extractor_binding: value.extractor_binding,
            output_index_mapping: value.output_index_mapping,
        }
    }
}

impl TryFrom<indexify_coordinator::Task> for Task {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::Task) -> Result<Self> {
        let content_metadata: ContentMetadata = value.content_metadata.unwrap().try_into()?;
        Ok(Self {
            id: value.id,
            extractor: value.extractor,
            repository: value.repository,
            content_metadata,
            input_params: serde_json::from_str(&value.input_params).unwrap(),
            extractor_binding: value.extractor_binding,
            output_index_mapping: value.output_index_mapping,
        })
    }
}

#[derive(Serialize, Debug, Deserialize, Display, Clone)]
pub enum ExtractionEventPayload {
    ExtractorBindingAdded {
        repository: String,
        binding: ExtractorBinding,
    },
    CreateContent {
        content: ContentMetadata,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractionEvent {
    pub id: String,
    pub repository: String,
    pub payload: ExtractionEventPayload,
    pub created_at: u64,
    pub processed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize)]
pub struct ExtractorBinding {
    pub id: String,
    pub name: String,
    pub repository: String,
    pub extractor: String,
    pub filters: HashMap<String, serde_json::Value>,
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
        let mut filters = HashMap::new();
        for filter in value.filters {
            filters.insert(filter.0, filter.1.to_string());
        }

        Self {
            extractor: value.extractor,
            name: value.name,
            filters,
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
            metadata.insert(key.to_owned(), serde_json::from_str(value)?);
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
    pub task_statuses: Vec<TaskResult>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WriteResponse {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResult {
    pub task_id: String,
    pub status: TaskState,
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
            bindings: value
                .extractor_bindings
                .into_iter()
                .map(|b| b.into())
                .collect(),
        }
    }
}
