use std::{collections::HashMap, sync::Arc, vec};

use anyhow::{anyhow, Result};
use axum::{
    extract::{multipart::Field, Multipart, State},
    response::Json,
};
use blob_store::{BlobStorage, PutResult};
use data_model::DataPayload;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use utoipa::ToSchema;

use super::RouteState;
use crate::http_objects::IndexifyAPIError;

#[derive(Serialize, Deserialize)]
pub enum TaskOutput {
    #[serde(rename = "router")]
    Router(RouterOutput),
    #[serde(rename = "fn")]
    Fn(FnOutput),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TaskOutcome {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "failure")]
    Failure,
}

impl From<TaskOutcome> for data_model::TaskOutcome {
    fn from(val: TaskOutcome) -> Self {
        match val {
            TaskOutcome::Success => data_model::TaskOutcome::Success,
            TaskOutcome::Failure => {
                data_model::TaskOutcome::Failure(data_model::TaskFailureReason::Unknown)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TaskResult {
    router_output: Option<RouterOutput>,
    outcome: TaskOutcome,
    namespace: String,
    compute_graph: String,
    compute_fn: String,
    task_id: String,
    invocation_id: String,
    executor_id: String,
    reducer: bool,
}

#[derive(Serialize, Deserialize)]
pub struct FnOutput {
    pub payload: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RouterOutput {
    pub edges: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IngestFnOutputsResponse {
    pub data_payloads: Vec<DataPayload>,
    pub stdout: Option<DataPayload>,
    pub stderr: Option<DataPayload>,
}

/// Upload data to a compute graph
#[utoipa::path(
    post,
    path = "internal/ingest_fn_outputs",
    request_body(content_type = "multipart/form-data", content = inline(InvokeWithFile)),
    tag = "ingestion",
    responses(
        (status = 200, description = "upload successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
pub async fn ingest_fn_outputs(
    State(state): State<RouteState>,
    mut files: Multipart,
) -> Result<Json<IngestFnOutputsResponse>, IndexifyAPIError> {
    let mut output_objects: Vec<PutResult> = vec![];
    let mut task_result: Option<TaskResult> = None;
    let mut stdout: Option<DataPayload> = None;
    let mut stderr: Option<DataPayload> = None;

    // Write data object to blob store.
    let mut node_output_sequence: usize = 0;
    let diagnostics_keys = ["stdout", "stderr"];
    while let Some(mut field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            let name_ref = name.to_string();
            if name_ref == "node_outputs" {
                let task_result = task_result.as_ref().ok_or_else(|| {
                    IndexifyAPIError::bad_request("task_result is required before node_outputs")
                })?;
                let mut file_name = format!(
                    "{}.{}.{}.{}",
                    task_result.namespace,
                    task_result.compute_graph,
                    task_result.compute_fn,
                    task_result.invocation_id,
                );
                if task_result.reducer {
                    file_name.push_str(&format!(".{}", node_output_sequence));
                } else {
                    file_name.push_str(&format!(
                        ".{}.{}",
                        task_result.task_id, node_output_sequence
                    ));
                };
                let res = write_to_disk(state.clone().blob_storage, &mut field, &file_name).await?;
                node_output_sequence += 1;
                output_objects.push(res.clone());
            } else if diagnostics_keys.iter().any(|e| name_ref.contains(e)) {
                let task_result = task_result.as_ref().ok_or_else(|| {
                    IndexifyAPIError::bad_request("task_result is required before diagnostics")
                })?;
                let file_name = format!(
                    "{}.{}.{}.{}.{}.{}",
                    task_result.namespace,
                    task_result.compute_graph,
                    task_result.compute_fn,
                    task_result.invocation_id,
                    task_result.task_id,
                    name,
                );
                let res = write_to_disk(state.clone().blob_storage, &mut field, &file_name).await?;
                match name_ref.as_str() {
                    "stdout" => {
                        stdout = Some(DataPayload {
                            path: res.url,
                            size: res.size_bytes,
                            sha256_hash: res.sha256_hash,
                        })
                    }
                    "stderr" => {
                        stderr = Some(DataPayload {
                            path: res.url,
                            size: res.size_bytes,
                            sha256_hash: res.sha256_hash,
                        })
                    }
                    _ => {
                        error!("unknown field name {}", name_ref);
                    }
                }
            } else if name == "task_result" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                task_result.replace(serde_json::from_str::<TaskResult>(&text)?);
            }
        }
    }

    return Ok(Json(IngestFnOutputsResponse {
        data_payloads: output_objects
            .iter()
            .map(|e| DataPayload {
                path: e.url.clone(),
                size: e.size_bytes,
                sha256_hash: e.sha256_hash.clone(),
            })
            .collect(),
        stdout,
        stderr,
    }));
}

#[allow(dead_code)]
#[derive(ToSchema)]
pub struct InvokeWithFile {
    /// Extra metadata for file
    metadata: Option<HashMap<String, serde_json::Value>>,
    #[schema(format = "binary")]
    /// File to upload
    file: Option<String>,
}

async fn write_to_disk<'a>(
    blob_storage: Arc<BlobStorage>,
    field: &'a mut Field<'a>,
    file_name: &str,
) -> Result<PutResult, IndexifyAPIError> {
    let _ = field
        .file_name()
        .as_ref()
        .ok_or(IndexifyAPIError::bad_request("file name is required"))?
        .to_string();
    debug!("writing to blob store, file name = {:?}", file_name);
    let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
    blob_storage.put(file_name, stream).await.map_err(|e| {
        error!("failed to write to blob store: {:?}", e);
        IndexifyAPIError::internal_error(anyhow!("failed to write to blob store: {}", e))
    })
}
