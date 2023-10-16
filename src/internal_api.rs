use serde::{Deserialize, Serialize};

use crate::persistence::{ExtractorConfig, Work};

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

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncExecutor {
    pub executor_id: String,
    pub extractor: ExtractorConfig,
    pub addr: String,
    pub work_status: Vec<Work>,
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
