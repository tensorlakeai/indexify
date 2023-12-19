use std::{collections::HashMap, str::FromStr};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};

use crate::{
    api,
    persistence::{self, EmbeddingSchema},
    vectordbs::IndexDistance,
};

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

impl TryFrom<ExtractorDescription> for persistence::Extractor {
    type Error = anyhow::Error;

    fn try_from(extractor: ExtractorDescription) -> Result<persistence::Extractor> {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.schema.output {
            match embedding_schema {
                OutputSchema::Embedding {
                    dim,
                    distance_metric,
                } => {
                    let distance = IndexDistance::from_str(&distance_metric)?;
                    output_schema.insert(
                        output_name,
                        persistence::ExtractorOutputSchema::Embedding(EmbeddingSchema {
                            dim,
                            distance,
                        }),
                    );
                }
                OutputSchema::Feature(schema) => {
                    output_schema.insert(
                        output_name,
                        persistence::ExtractorOutputSchema::Attributes(
                            persistence::MetadataSchema { schema },
                        ),
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

impl From<persistence::Extractor> for ExtractorDescription {
    fn from(extractor: persistence::Extractor) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in extractor.schemas.outputs {
            match embedding_schema {
                persistence::ExtractorOutputSchema::Embedding(schema) => {
                    let distance_metric = schema.distance.to_string();
                    output_schema.insert(
                        output_name,
                        OutputSchema::Embedding {
                            dim: schema.dim,
                            distance_metric,
                        },
                    );
                }
                persistence::ExtractorOutputSchema::Attributes(schema) => {
                    output_schema.insert(output_name, OutputSchema::Feature(schema.schema));
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
