use anyhow::Result;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use std::str::FromStr;
use strum_macros::{Display, EnumString};

use crate::persistence::{self, ExtractorConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorInfo {
    pub id: String,
    pub last_seen: u64,
    pub addr: String,
    pub extractor: ExtractorConfig,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedQueryRequest {
    pub extractor_name: String,
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbedQueryResponse {
    pub embedding: Vec<f32>,
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
    pub data: Option<ExtractedContent>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncExecutor {
    pub executor_id: String,
    pub extractor: ExtractorConfig,
    pub addr: String,
    pub work_status: Vec<WorkStatus>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListExecutors {
    pub executors: Vec<ExecutorInfo>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ListExtractors {
    pub extractors: Vec<ExtractorConfig>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ExtractedData {
    Embeddings { embedding: Vec<f32> },
    Attributes { attributes: serde_json::Value },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractedContent {
    pub content_id: String,
    pub source: String,
    pub extracted_data: ExtractedData,
}

#[derive(Clone, Debug, Display, EnumString, Serialize, Deserialize)]
pub enum ContentType {
    #[strum(serialize = "text")]
    Text,

    #[strum(serialize = "pdf")]
    Pdf,
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
    pub content_id: String,
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
        content_id: work.content_id,
        content_payload,
        params: work.extractor_params,
    })
}
