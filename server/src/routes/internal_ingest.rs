use std::{collections::HashMap, sync::Arc, vec};

use anyhow::{anyhow, Result};
use axum::extract::{multipart::Field, Multipart, State};
use blob_store::{BlobStorage, PutResult};
use data_model::{
    DataPayload,
    ExecutorId,
    NodeOutput,
    NodeOutputBuilder,
    OutputPayload,
    TaskDiagnostics,
    TaskId,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use state_store::requests::{FinalizeTaskRequest, RequestPayload, StateMachineUpdateRequest};
use tracing::{error, info};
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
            TaskOutcome::Failure => data_model::TaskOutcome::Failure,
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
    path = "internal/ingest_files",
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
) -> Result<(), IndexifyAPIError> {
    let mut output_objects: Vec<PutResult> = vec![];
    let mut output_encoding: Vec<String> = vec![];
    let mut stdout_msg: Option<PutResult> = None;
    let mut stderr_msg: Option<PutResult> = None;
    let mut task_result: Option<TaskResult> = None;

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
                // If there is no content_type, set it as octet-stream.
                output_encoding.push(
                    field
                        .content_type()
                        .unwrap_or_else(|| "application/octet-stream")
                        .to_string(),
                );
                let res = write_to_disk(state.clone().blob_storage, &mut field, &file_name).await?;
                node_output_sequence += 1;
                output_objects.push(res.clone());
            } else if diagnostics_keys.iter().any(|e| name_ref.contains(e)) {
                let task_result = task_result.as_ref().ok_or_else(|| {
                    IndexifyAPIError::bad_request("task_result is required before diagnostics")
                })?;
                let file_name = format!(
                    "{}.{}.{}.{}.{}",
                    task_result.namespace,
                    task_result.compute_graph,
                    task_result.compute_fn,
                    task_result.invocation_id,
                    name,
                );
                let res = write_to_disk(state.clone().blob_storage, &mut field, &file_name).await?;
                match name_ref.as_str() {
                    "stdout" => stdout_msg = Some(res),
                    "stderr" => stderr_msg = Some(res),
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

    state
        .metrics
        .fn_outputs
        .add(output_objects.len() as u64, &[]);
    state.metrics.fn_output_bytes.add(
        output_objects.iter().map(|e| e.size_bytes).sum::<u64>(),
        &[],
    );

    // Save metadata in rocksdb for the objects in the blob store.
    let task_result =
        task_result.ok_or(IndexifyAPIError::bad_request("task_result is required"))?;
    let mut node_outputs: Vec<NodeOutput> = vec![];

    for (index, put_result) in output_objects.iter().enumerate() {
        let data_payload = data_model::DataPayload {
            path: put_result.clone().url,
            size: put_result.clone().size_bytes,
            sha256_hash: put_result.clone().sha256_hash,
        };
        let node_output = NodeOutputBuilder::default()
            .namespace(task_result.namespace.to_string())
            .graph_version(Default::default())
            .compute_graph_name(task_result.compute_graph.to_string())
            .invocation_id(task_result.invocation_id.to_string())
            .compute_fn_name(task_result.compute_fn.to_string())
            .payload(OutputPayload::Fn(data_payload))
            .encoding(output_encoding[index].to_string())
            .build()
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
            })?;
        node_outputs.push(node_output);
    }

    let stdout_payload = prepare_data_payload(stdout_msg);
    let stderr_payload = prepare_data_payload(stderr_msg);

    let task_diagnostic = TaskDiagnostics {
        stdout: stdout_payload,
        stderr: stderr_payload,
    };

    if let Some(router_output) = task_result.router_output {
        let node_output = NodeOutputBuilder::default()
            .namespace(task_result.namespace.to_string())
            .graph_version(Default::default())
            .compute_graph_name(task_result.compute_graph.to_string())
            .invocation_id(task_result.invocation_id.to_string())
            .compute_fn_name(task_result.compute_fn.to_string())
            .payload(OutputPayload::Router(data_model::RouterOutput {
                edges: router_output.edges.clone(),
            }))
            .build()
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
            })?;
        node_outputs.push(node_output);
    }

    let request = RequestPayload::FinalizeTask(FinalizeTaskRequest {
        namespace: task_result.namespace.to_string(),
        compute_graph: task_result.compute_graph.to_string(),
        compute_fn: task_result.compute_fn.to_string(),
        invocation_id: task_result.invocation_id.to_string(),
        task_id: TaskId::new(task_result.task_id.to_string()),
        node_outputs,
        task_outcome: task_result.outcome.clone().into(),
        executor_id: ExecutorId::new(task_result.executor_id.clone()),
        diagnostics: Some(task_diagnostic),
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
    info!("writing to blob store, file name = {:?}", file_name);
    let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
    blob_storage.put(file_name, stream).await.map_err(|e| {
        error!("failed to write to blob store: {:?}", e);
        IndexifyAPIError::internal_error(anyhow!("failed to write to blob store: {}", e))
    })
}

fn prepare_data_payload(msg: Option<PutResult>) -> Option<DataPayload> {
    msg.map(|msg| DataPayload {
        path: msg.url,
        size: msg.size_bytes,
        sha256_hash: msg.sha256_hash,
    })
}
