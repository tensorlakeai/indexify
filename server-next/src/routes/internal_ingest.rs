use std::{collections::HashMap, vec};

use anyhow::{anyhow, Result};
use axum::extract::{Multipart, State};
use blob_store::PutResult;
use data_model::{ExecutorId, NodeOutput, NodeOutputBuilder, OutputPayload, TaskId};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use state_store::requests::{FinalizeTaskRequest, RequestPayload, StateMachineUpdateRequest};
use tracing::info;
use utoipa::ToSchema;
use uuid::Uuid;

use super::RouteState;
use crate::http_objects::IndexifyAPIError;

#[derive(Serialize, Deserialize)]
pub enum TaskOutput {
    #[serde(rename = "router")]
    Router(RouterOutput),
    #[serde(rename = "fn")]
    Fn(FnOutput),
}

#[derive(Serialize, Deserialize, Clone)]
pub enum TaskOutcome {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "failure")]
    Failure,
}

impl Into<data_model::TaskOutcome> for TaskOutcome {
    fn into(self) -> data_model::TaskOutcome {
        match self {
            TaskOutcome::Success => data_model::TaskOutcome::Success,
            TaskOutcome::Failure => data_model::TaskOutcome::Failure,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TaskResult {
    router_outputs: Vec<RouterOutput>,
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
) -> Result<(), IndexifyAPIError> {
    let mut output_objects: Vec<PutResult> = vec![];
    let mut task_result: Option<TaskResult> = None;
    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "node_outputs" {
                let _ = field
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
                output_objects.push(res.clone());
            } else if name == "task_result" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                task_result.replace(serde_json::from_str::<TaskResult>(&text)?);
            }
        }
    }
    let task_result =
        task_result.ok_or(IndexifyAPIError::bad_request("task_result is required"))?;
    let mut node_outputs: Vec<NodeOutput> = vec![];
    for put_result in output_objects {
        let data_payload = data_model::DataPayload {
            path: put_result.url,
            size: put_result.size_bytes,
            sha256_hash: put_result.sha256_hash,
        };
        let node_output = NodeOutputBuilder::default()
            .namespace(task_result.namespace.to_string())
            .compute_graph_name(task_result.compute_graph.to_string())
            .invocation_id(task_result.invocation_id.to_string())
            .compute_fn_name(task_result.compute_fn.to_string())
            .payload(OutputPayload::Fn(data_payload))
            .build()
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
            })?;
        node_outputs.push(node_output);
    }
    for router_outputs in task_result.router_outputs {
        let node_output = NodeOutputBuilder::default()
            .namespace(task_result.namespace.to_string())
            .compute_graph_name(task_result.compute_graph.to_string())
            .invocation_id(task_result.invocation_id.to_string())
            .compute_fn_name(task_result.compute_fn.to_string())
            .payload(OutputPayload::Router(data_model::RouterOutput {
                edges: router_outputs.edges.clone(),
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
