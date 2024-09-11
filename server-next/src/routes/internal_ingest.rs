use std::{collections::HashMap, vec};

use anyhow::{anyhow, Result};
use axum::{
    extract::{Multipart, State},
    Json,
};
use data_model::{ExecutorId, NodeOutput, NodeOutputBuilder, OutputPayload, TaskId, TaskOutcome};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use state_store::requests::{FinalizeTaskRequest, RequestPayload, StateMachineUpdateRequest};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

use super::RouteState;
use crate::http_objects::IndexifyAPIError;

#[derive(Serialize, Deserialize)]
pub struct FnOutputBinary {
    pub path: String,
    pub size: u64,
    pub sha256_hash: String,
}

#[derive(Serialize, Deserialize)]
pub enum TaskOutput {
    #[serde(rename = "router")]
    Router(RouterOutput),
    #[serde(rename = "fn")]
    Fn(FnOutput),
}

#[derive(Serialize, Deserialize)]
pub struct TaskResult {
    outputs: Vec<TaskOutput>,
    outcome: TaskOutcome,
    namespace: String,
    compute_graph: String,
    compute_fn: String,
    task_id: String,
    invocation_id: String,
    executor_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct FnOutput {
    pub payload: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
pub struct RouterOutput {
    pub edges: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ExecutorFileUploadResponse {
    pub files: HashMap<String, FnOutputBinary>,
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
/// Upload data to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_file",
    request_body(content_type = "multipart/form-data", content = inline(InvokeWithFile)),
    tag = "ingestion",
    responses(
        (status = 200, description = "upload successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn ingest_files_from_executor(
    State(state): State<RouteState>,
    mut files: Multipart,
) -> Result<Json<ExecutorFileUploadResponse>, IndexifyAPIError> {
    let mut response = HashMap::new();
    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "file" {
                let file_name = field
                    .file_name()
                    .as_ref()
                    .ok_or(IndexifyAPIError::bad_request("file name is required"))?
                    .to_string();
                let name = Uuid::new_v4().to_string();
                info!("writing to blob store, file name = {:?}", name);
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
                let res = state.blob_storage.put(&name, stream).await.map_err(|e| {
                    IndexifyAPIError::internal_error(anyhow!(
                        "failed to write to blob store: {}",
                        e
                    ))
                })?;
                response.insert(
                    file_name,
                    FnOutputBinary {
                        path: res.url,
                        size: res.size_bytes,
                        sha256_hash: res.sha256_hash,
                    },
                );
            }
        }
    }
    Ok(Json(ExecutorFileUploadResponse { files: response }))
}

/// Upload JSON serialized object to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invoke_object",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "invocation successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn ingest_objects_from_executor(
    State(state): State<RouteState>,
    Json(task_result): Json<TaskResult>,
) -> Result<(), IndexifyAPIError> {
    let mut node_outputs = vec![];
    for output in &task_result.outputs {
        let node_output = match output {
            TaskOutput::Router(output) => write_router_output(output, &task_result)?,
            TaskOutput::Fn(fn_output) => {
                write_fn_output(state.clone(), fn_output, &task_result).await?
            }
        };
        node_outputs.push(node_output);
    }
    let request = RequestPayload::FinalizeTask(FinalizeTaskRequest {
        namespace: task_result.compute_graph.to_string(),
        compute_graph: task_result.compute_graph.to_string(),
        compute_fn: task_result.compute_fn.to_string(),
        invocation_id: task_result.invocation_id.to_string(),
        task_id: TaskId::new(task_result.task_id.to_string()),
        node_outputs,
        task_outcome: task_result.outcome.clone(),
        executor_id: ExecutorId::new(task_result.executor_id.clone()),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    Ok(())
}

fn write_router_output(
    output: &RouterOutput,
    task_result: &TaskResult,
) -> Result<NodeOutput, IndexifyAPIError> {
    let node_output = NodeOutputBuilder::default()
        .namespace(task_result.namespace.to_string())
        .compute_graph_name(task_result.compute_graph.to_string())
        .compute_fn_name(task_result.compute_fn.to_string())
        .payload(OutputPayload::Router(data_model::RouterOutput {
            edges: output.edges.clone(),
        }))
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    Ok(node_output)
}

async fn write_fn_output(
    state: RouteState,
    fn_output: &FnOutput,
    task_result: &TaskResult,
) -> Result<NodeOutput, IndexifyAPIError> {
    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = stream::once(async move {
        let payload_json = serde_json::to_string(&fn_output.payload)?
            .as_bytes()
            .to_vec()
            .clone();
        Ok(payload_json.into())
    });
    let put_result = state
        .blob_storage
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_payload = data_model::DataPayload {
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
    };
    let node_output = NodeOutputBuilder::default()
        .namespace(task_result.namespace.to_string())
        .compute_graph_name(task_result.compute_graph.to_string())
        .compute_fn_name(task_result.compute_fn.to_string())
        .payload(OutputPayload::Fn(data_payload))
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    Ok(node_output)
}
