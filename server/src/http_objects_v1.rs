use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    config::ExecutorConfig,
    data_model::{
        self,
        ComputeGraphCode,
        GraphInvocationCtx,
        GraphInvocationFailureReason,
        GraphInvocationOutcome,
    },
    http_objects::{
        self,
        ComputeFn,
        GraphVersion,
        IndexifyAPIError,
        RequestError,
        TaskOutcome,
        TaskStatus,
    },
    utils::get_epoch_time_in_ms,
};

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub description: String,
    #[serde(default)]
    pub tombstoned: bool,
    pub entrypoint: ComputeFn,
    pub version: GraphVersion,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    pub functions: HashMap<String, ComputeFn>,
    pub edges: HashMap<String, Vec<String>>,
    #[serde(default = "get_epoch_time_in_ms")]
    pub created_at: u64,
    pub runtime_information: http_objects::RuntimeInformation,
}

impl ComputeGraph {
    pub fn into_data_model(
        self,
        code_path: &str,
        sha256_hash: &str,
        size: u64,
        executor_config: &ExecutorConfig,
    ) -> Result<data_model::ComputeGraph, IndexifyAPIError> {
        let mut nodes = HashMap::new();
        for (name, node) in self.functions {
            node.validate(executor_config)?;
            nodes.insert(name, node.into());
        }
        let start_fn: data_model::ComputeFn = self.entrypoint.into();

        let compute_graph = data_model::ComputeGraph {
            name: self.name,
            namespace: self.namespace,
            description: self.description,
            start_fn,
            tags: self.tags.unwrap_or_default(),
            version: self.version.into(),
            code: ComputeGraphCode {
                sha256_hash: sha256_hash.to_string(),
                size,
                path: code_path.to_string(),
            },
            nodes,
            edges: self.edges.clone(),
            created_at: 0,
            runtime_information: self.runtime_information.into(),
            tombstoned: self.tombstoned,
        };
        Ok(compute_graph)
    }
}

impl From<data_model::ComputeGraph> for ComputeGraph {
    fn from(compute_graph: data_model::ComputeGraph) -> Self {
        let start_fn = compute_graph.start_fn.into();
        let mut nodes = HashMap::new();
        for (k, v) in compute_graph.nodes.into_iter() {
            nodes.insert(k, v.into());
        }
        Self {
            name: compute_graph.name,
            namespace: compute_graph.namespace,
            description: compute_graph.description,
            entrypoint: start_fn,
            tags: Some(compute_graph.tags),
            version: compute_graph.version.into(),
            functions: nodes,
            edges: compute_graph.edges,
            created_at: compute_graph.created_at,
            runtime_information: compute_graph.runtime_information.into(),
            tombstoned: compute_graph.tombstoned,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraphsList {
    pub compute_graphs: Vec<ComputeGraph>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShallowGraphRequest {
    pub id: String,
    pub created_at: u64,
    pub status: RequestStatus,
    pub outcome: RequestOutcome,
}

impl From<GraphInvocationCtx> for ShallowGraphRequest {
    fn from(ctx: GraphInvocationCtx) -> Self {
        Self {
            id: ctx.invocation_id.to_string(),
            created_at: ctx.created_at,
            status: if ctx.completed {
                RequestStatus::Finalized
            } else if ctx.outstanding_tasks > 0 {
                RequestStatus::Running
            } else {
                RequestStatus::Pending
            },
            outcome: ctx.outcome.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GraphRequests {
    pub requests: Vec<ShallowGraphRequest>,
    pub prev_cursor: Option<String>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Task {
    pub id: String,
    pub status: TaskStatus,
    pub outcome: TaskOutcome,
    pub graph_version: GraphVersion,
    pub allocations: Vec<Allocation>,
    pub created_at: u128,
}

impl Task {
    pub fn from_data_model_task(task: data_model::Task, allocations: Vec<Allocation>) -> Self {
        Self {
            id: task.id.to_string(),
            outcome: task.outcome.into(),
            status: task.status.into(),
            graph_version: task.graph_version.into(),
            allocations,
            created_at: task.creation_time_ns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Tasks {
    pub tasks: Vec<Task>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum RequestStatus {
    Pending,
    Running,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum RequestOutcome {
    Undefined,
    Success,
    Failure,
}

impl From<GraphInvocationOutcome> for RequestOutcome {
    fn from(outcome: GraphInvocationOutcome) -> Self {
        match outcome {
            GraphInvocationOutcome::Unknown => RequestOutcome::Undefined,
            GraphInvocationOutcome::Success => RequestOutcome::Success,
            GraphInvocationOutcome::Failure(_) => RequestOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum RequestFailureReason {
    Unknown,
    InternalError,
    FunctionError,
    InvocationError,
    NextFunctionNotFound,
}

impl From<GraphInvocationFailureReason> for RequestFailureReason {
    fn from(failure_reason: GraphInvocationFailureReason) -> Self {
        match failure_reason {
            GraphInvocationFailureReason::Unknown => RequestFailureReason::Unknown,
            GraphInvocationFailureReason::InternalError => RequestFailureReason::InternalError,
            GraphInvocationFailureReason::FunctionError => RequestFailureReason::FunctionError,
            GraphInvocationFailureReason::InvocationError => RequestFailureReason::InvocationError,
            GraphInvocationFailureReason::NextFunctionNotFound => {
                RequestFailureReason::NextFunctionNotFound
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FnOutput {
    pub id: String,
    pub num_outputs: u64,
    pub compute_fn: String,
    pub created_at: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Request {
    pub id: String,
    pub completed: bool,
    pub status: RequestStatus,
    pub outcome: RequestOutcome,
    pub failure_reason: RequestFailureReason,
    pub outstanding_tasks: u64,
    pub request_progress: HashMap<String, RequestProgress>,
    pub graph_version: String,
    pub created_at: u64,
    pub request_error: Option<RequestError>,
    pub outputs: Vec<FnOutput>,
}

impl Request {
    pub fn build(
        ctx: GraphInvocationCtx,
        outputs: Vec<FnOutput>,
        invocation_error: Option<RequestError>,
    ) -> Self {
        let mut task_analytics = HashMap::new();
        for (k, v) in ctx.fn_task_analytics {
            task_analytics.insert(
                k,
                RequestProgress {
                    pending_tasks: v.pending_tasks,
                    successful_tasks: v.successful_tasks,
                    failed_tasks: v.failed_tasks,
                },
            );
        }
        let status = if ctx.completed {
            RequestStatus::Finalized
        } else if ctx.outstanding_tasks > 0 {
            RequestStatus::Running
        } else {
            RequestStatus::Pending
        };
        Self {
            id: ctx.invocation_id.to_string(),
            completed: ctx.completed,
            outcome: ctx.outcome.clone().into(),
            failure_reason: match &ctx.outcome {
                GraphInvocationOutcome::Failure(reason) => reason.clone().into(),
                _ => GraphInvocationFailureReason::Unknown.into(),
            },
            status,
            outstanding_tasks: ctx.outstanding_tasks,
            request_progress: task_analytics,
            graph_version: ctx.graph_version.0,
            created_at: ctx.created_at,
            request_error: invocation_error,
            outputs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RequestProgress {
    pub pending_tasks: u64,
    pub successful_tasks: u64,
    pub failed_tasks: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Allocation {
    pub id: String,
    pub executor_id: String,
    pub function_executor_id: String,
    pub created_at: u128,
    pub outcome: TaskOutcome,
    pub attempt_number: u32,
    pub execution_duration_ms: Option<u64>,
}

impl From<data_model::Allocation> for Allocation {
    fn from(allocation: data_model::Allocation) -> Self {
        Self {
            id: allocation.id.to_string(),
            executor_id: allocation.target.executor_id.to_string(),
            function_executor_id: allocation.target.function_executor_id.get().to_string(),
            created_at: allocation.created_at,
            outcome: allocation.outcome.into(),
            attempt_number: allocation.attempt_number,
            execution_duration_ms: allocation.execution_duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::http_objects::ComputeFn;

    #[test]
    fn test_compute_graph_deserialization() {
        // Don't delete this. It makes it easier
        // to test the deserialization of the ComputeGraph struct
        // from the python side
        let json = r#"{"name":"test","description":"test","entrypoint":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"},"functions":{"extractor_a":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle","image_name": "default_image"},"extractor_b":{"name":"extractor_b","fn_name":"extractor_b","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"},"extractor_c":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"edges":{"extractor_a":["extractor_b"],"extractor_b":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10, "sdk_version": "1.2.3"}, "version": "1.2.3"}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_fn_deserialization() {
        let json = r#"{"name": "one", "fn_name": "two", "description": "desc", "reducer": true, "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle"}"#;
        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        println!("{:?}", compute_fn);
    }
}
