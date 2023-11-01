use anyhow::Result;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use std::{collections::HashMap, str::FromStr};
use strum_macros::{Display, EnumString};

use crate::{persistence, vectordbs::IndexDistance};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputSchema {
    Embedding { dim: usize, distance_metric: String },
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

impl TryFrom<ExtractorDescription> for persistence::ExtractorDescription {
    type Error = anyhow::Error;
    fn try_from(extractor: ExtractorDescription) -> Result<persistence::ExtractorDescription> {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.schema.output {
            match embedding_schema {
                OutputSchema::Embedding {
                    dim,
                    distance_metric,
                } => {
                    let distance_metric = IndexDistance::from_str(&distance_metric)?;
                    output_schema.insert(
                        output_name,
                        persistence::ExtractorOutputSchema::Embedding {
                            dim,
                            distance_metric,
                        },
                    );
                }
                OutputSchema::Feature(feature_schema) => {
                    output_schema.insert(
                        output_name,
                        persistence::ExtractorOutputSchema::Attributes(feature_schema),
                    );
                }
            }
        }
        Ok(Self {
            name: extractor.name,
            description: extractor.description,
            input_params: extractor.input_params,
            schemas: persistence::ExtractorSchema {
                outputs: output_schema,
            },
        })
    }
}

impl From<persistence::ExtractorDescription> for ExtractorDescription {
    fn from(extractor: persistence::ExtractorDescription) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.schemas.outputs {
            match embedding_schema {
                persistence::ExtractorOutputSchema::Embedding {
                    dim,
                    distance_metric,
                } => {
                    let distance_metric = distance_metric.to_string();
                    output_schema.insert(
                        output_name,
                        OutputSchema::Embedding {
                            dim,
                            distance_metric,
                        },
                    );
                }
                persistence::ExtractorOutputSchema::Attributes(feature_schema) => {
                    output_schema.insert(output_name, OutputSchema::Feature(feature_schema));
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
    pub extractor_name: String,
    pub content: ExtractedContent,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractResponse {
    pub content: Vec<ExtractedContent>,
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
    pub extracted_content: Vec<ExtractedContent>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncExecutor {
    pub executor_id: String,
    pub extractor: ExtractorDescription,
    pub addr: String,
    pub work_status: Vec<WorkStatus>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListExecutors {
    pub executors: Vec<ExecutorInfo>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListExtractors {
    pub extractors: Vec<ExtractorDescription>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractedContent {
    pub content_type: ContentType,
    pub source: Vec<u8>,
    pub feature: Option<Feature>,
}

impl ExtractedContent {
    pub fn source_as_text(&self) -> Option<String> {
        match self.content_type {
            ContentType::Text => Some(String::from_utf8(self.source.clone()).unwrap()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Display, EnumString, Serialize, Deserialize)]
pub enum ContentType {
    #[strum(serialize = "text")]
    Text,

    #[strum(serialize = "pdf")]
    Pdf,

    #[strum(serialize = "audio")]
    Audio,

    #[strum(serialize = "image")]
    Image,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContentPayload {
    pub content_type: ContentType,
    pub content: String,
    pub external_url: Option<String>,
}

impl TryFrom<persistence::ContentPayload> for ContentPayload {
    type Error = anyhow::Error;
    fn try_from(payload: persistence::ContentPayload) -> Result<Self> {
        let _content_type = ContentType::from_str(&payload.content_type.to_string());
        let (external_url, content) = match payload.payload_type {
            persistence::PayloadType::BlobStorageLink => (Some(payload.payload), "".to_string()),
            _ => (None, payload.payload),
        };
        Ok(Self {
            content_type: ContentType::Text,
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

pub fn create_work(
    work: persistence::Work,
    content_payload: persistence::ContentPayload,
) -> Result<Work> {
    let content_payload = ContentPayload::try_from(content_payload)?;
    Ok(Work {
        id: work.id,
        content_payload,
        params: work.extractor_params,
    })
}
