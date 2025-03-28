#[allow(non_camel_case_types)] // The autogenerated code uses snake_case types in some cases
pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

use std::{collections::HashMap, pin::Pin, sync::Arc};

use data_model::{
    DataPayload,
    ExecutorId,
    ExecutorMetadata,
    ExecutorMetadataBuilder,
    FunctionExecutor,
    FunctionExecutorStatus,
    FunctionURI,
    GpuResources,
    GraphVersion,
    HostResources,
    NodeOutputBuilder,
    OutputPayload,
    TaskDiagnostics,
    TaskOutcome,
    TaskOutputsIngestionStatus,
    TaskStatus,
};
use executor_api_pb::{
    executor_api_server::ExecutorApi,
    AllowedFunction,
    DesiredExecutorState,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorDescription,
    GetDesiredExecutorStatesRequest,
    GpuModel,
    OutputEncoding,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
    ReportTaskOutcomeRequest,
    ReportTaskOutcomeResponse,
};
use metrics::api_io_stats;
use state_store::{
    requests::{IngestTaskOutputsRequest, RequestPayload, StateMachineUpdateRequest},
    IndexifyState,
};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::executors::ExecutorManager;

impl TryFrom<AllowedFunction> for FunctionURI {
    type Error = anyhow::Error;

    fn try_from(allowed_function: AllowedFunction) -> Result<Self, Self::Error> {
        let namespace = allowed_function
            .namespace
            .ok_or(anyhow::anyhow!("namespace is required"))?;
        let compute_graph_name = allowed_function
            .graph_name
            .ok_or(anyhow::anyhow!("compute_graph_name is required"))?;
        let compute_fn_name = allowed_function
            .function_name
            .ok_or(anyhow::anyhow!("compute_fn_name is required"))?;
        let version = allowed_function.graph_version.map(|v| GraphVersion(v));
        Ok(FunctionURI {
            namespace,
            compute_graph_name,
            compute_fn_name,
            version,
        })
    }
}

impl From<GpuModel> for String {
    fn from(gpu_model: GpuModel) -> Self {
        match gpu_model {
            GpuModel::NvidiaTeslaT416gb => "T4".to_string(),
            GpuModel::NvidiaTeslaV10016gb => "V100".to_string(),
            GpuModel::NvidiaA1024gb => "A10".to_string(),
            GpuModel::NvidiaA600048gb => "A6000".to_string(),
            GpuModel::NvidiaA100Sxm440gb => "A100".to_string(),
            GpuModel::NvidiaA100Sxm480gb => "A100".to_string(),
            GpuModel::NvidiaA100Pci40gb => "A100".to_string(),
            GpuModel::NvidiaH100Sxm580gb => "H100".to_string(),
            GpuModel::NvidiaH100Pci80gb => "H100".to_string(),
            GpuModel::NvidiaRtx600024gb => "RTX6000".to_string(),
            GpuModel::Unknown => "Unknown".to_string(),
        }
    }
}

impl From<ExecutorStatus> for data_model::ExecutorState {
    fn from(status: ExecutorStatus) -> Self {
        match status {
            ExecutorStatus::StartingUp => data_model::ExecutorState::StartingUp,
            ExecutorStatus::Running => data_model::ExecutorState::Running,
            ExecutorStatus::Drained => data_model::ExecutorState::Drained,
            ExecutorStatus::Stopping => data_model::ExecutorState::Stopping,
            ExecutorStatus::Stopped => data_model::ExecutorState::Stopped,
            ExecutorStatus::Unknown => data_model::ExecutorState::Unknown,
        }
    }
}

impl TryFrom<ExecutorState> for ExecutorMetadata {
    type Error = anyhow::Error;

    fn try_from(executor_state: ExecutorState) -> Result<Self, Self::Error> {
        let mut executor_metadata = ExecutorMetadataBuilder::default();
        executor_metadata.state(executor_state.status().into());
        if let Some(executor_id) = executor_state.executor_id {
            executor_metadata.id(ExecutorId::new(executor_id));
        }
        // FIXME: ignoring Executor flavor for now.
        if let Some(executor_version) = executor_state.version {
            executor_metadata.executor_version(executor_version);
        }
        let mut allowed_functions = Vec::new();
        for function in executor_state.allowed_functions {
            allowed_functions.push(FunctionURI::try_from(function)?);
        }
        if allowed_functions.is_empty() {
            executor_metadata.function_allowlist(None);
        } else {
            executor_metadata.function_allowlist(Some(allowed_functions));
        }
        if let Some(addr) = executor_state.hostname {
            executor_metadata.addr(addr);
        }
        let mut labels = HashMap::new();
        for (key, value) in executor_state.labels {
            labels.insert(key, serde_json::Value::String(value));
        }
        executor_metadata.labels(labels);
        let mut function_executors = HashMap::new();
        for function_executor in executor_state.function_executor_states {
            let function_executor_description = function_executor
                .description
                .ok_or(anyhow::anyhow!("description is required"))?;
            let mut function_executor = FunctionExecutor::try_from(function_executor_description)?;
            function_executor.status = FunctionExecutorStatus::try_from(function_executor.status)?;
            function_executors.insert(function_executor.id.clone(), function_executor);
        }
        executor_metadata.function_executors(function_executors);
        if let Some(host_resources) = executor_state.free_resources {
            let cpu = host_resources
                .cpu_count
                .ok_or(anyhow::anyhow!("cpu_count is required"))?;
            let memory = host_resources
                .memory_bytes
                .ok_or(anyhow::anyhow!("memory_bytes is required"))?;
            let disk = host_resources
                .disk_bytes
                .ok_or(anyhow::anyhow!("disk_bytes is required"))?;
            let gpu = host_resources.gpu.map(|g| GpuResources {
                count: g.count(),
                model: g.model().into(),
            });
            executor_metadata.host_resources(HostResources {
                cpu_count: cpu,
                memory_bytes: memory,
                disk_bytes: disk,
                gpu,
            });
        }
        Ok(executor_metadata.build()?)
    }
}

impl TryFrom<FunctionExecutorDescription> for FunctionExecutor {
    type Error = anyhow::Error;

    fn try_from(
        function_executor_description: FunctionExecutorDescription,
    ) -> Result<Self, Self::Error> {
        let id = function_executor_description
            .id
            .ok_or(anyhow::anyhow!("id is required"))?;
        let namespace = function_executor_description
            .namespace
            .ok_or(anyhow::anyhow!("namespace is required"))?;
        let compute_graph_name = function_executor_description
            .graph_name
            .ok_or(anyhow::anyhow!("compute_graph_name is required"))?;
        let compute_fn_name = function_executor_description
            .function_name
            .ok_or(anyhow::anyhow!("compute_fn_name is required"))?;
        let version = function_executor_description
            .graph_version
            .map(GraphVersion)
            .ok_or(anyhow::anyhow!("version is required"))?;

        Ok(FunctionExecutor {
            id,
            namespace,
            compute_graph_name,
            compute_fn_name,
            version,
            status: FunctionExecutorStatus::Unknown,
        })
    }
}

pub struct ExecutorAPIService {
    indexify_state: Arc<IndexifyState>,
    executor_manager: Arc<ExecutorManager>,
    api_metrics: Arc<api_io_stats::Metrics>,
}

impl ExecutorAPIService {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        executor_manager: Arc<ExecutorManager>,
        api_metrics: Arc<api_io_stats::Metrics>,
    ) -> Self {
        Self {
            indexify_state,
            executor_manager,
            api_metrics,
        }
    }
}

#[tonic::async_trait]
impl ExecutorApi for ExecutorAPIService {
    #[allow(non_camel_case_types)] // The autogenerated code in the trait uses snake_case types in some cases
    type get_desired_executor_statesStream =
        Pin<Box<dyn Stream<Item = Result<DesiredExecutorState, Status>> + Send>>;

    async fn report_executor_state(
        &self,
        request: Request<ReportExecutorStateRequest>,
    ) -> Result<Response<ReportExecutorStateResponse>, Status> {
        let executor_state = request
            .get_ref()
            .executor_state
            .clone()
            .ok_or(Status::invalid_argument("executor_state is required"))?;
        debug!(
            "Got report_executor_state request from Executor with ID {}",
            executor_state.executor_id()
        );
        let executor_metadata = ExecutorMetadata::try_from(executor_state)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.executor_manager
            .heartbeat(executor_metadata)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ReportExecutorStateResponse {}))
    }

    async fn get_desired_executor_states(
        &self,
        request: Request<GetDesiredExecutorStatesRequest>,
    ) -> Result<Response<Self::get_desired_executor_statesStream>, Status> {
        info!(
            "Got get_desired_executor_states request from Executor with ID {}",
            request.get_ref().executor_id()
        );

        // Based on https://github.com/hyperium/tonic/blob/72b0fd59442d71804d4104e313ef6f140ab8f6d1/examples/src/streaming/server.rs#L46
        // creating infinite stream with fake message
        let repeat = std::iter::repeat(DesiredExecutorState {
            function_executors: vec![],
            task_allocations: vec![],
            clock: Some(3),
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat));

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to
                        // client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            info!("get_desired_executor_states stream finished, client disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::get_desired_executor_statesStream
        ))
    }

    async fn report_task_outcome(
        &self,
        request: Request<ReportTaskOutcomeRequest>,
    ) -> Result<Response<ReportTaskOutcomeResponse>, Status> {
        self.api_metrics
            .fn_outputs
            .add(request.get_ref().fn_outputs.len() as u64, &[]);

        let task_id = request
            .get_ref()
            .task_id
            .clone()
            .ok_or(Status::invalid_argument("task_id is required"))?;

        let task_outcome =
            executor_api_pb::TaskOutcome::try_from(request.get_ref().outcome.unwrap_or(0))
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let executor_id = request
            .get_ref()
            .executor_id
            .clone()
            .ok_or(Status::invalid_argument("executor_id is required"))?;
        let namespace = request
            .get_ref()
            .namespace
            .clone()
            .ok_or(Status::invalid_argument("namespace is required"))?;
        let compute_graph = request
            .get_ref()
            .graph_name
            .clone()
            .ok_or(Status::invalid_argument("compute_graph is required"))?;
        let compute_fn = request
            .get_ref()
            .function_name
            .clone()
            .ok_or(Status::invalid_argument("compute_fn is required"))?;
        let invocation_id = request
            .get_ref()
            .invocation_id
            .clone()
            .ok_or(Status::invalid_argument("invocation_id is required"))?;
        let task = self
            .indexify_state
            .reader()
            .get_task(
                &namespace,
                &compute_graph,
                &invocation_id,
                &compute_fn,
                &task_id,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        let output_encoding = request
            .get_ref()
            .output_encoding
            .ok_or(Status::invalid_argument("output_encoding is required"))?;
        if task.is_none() {
            warn!("Task not found for task_id: {}", task_id);
            return Ok(Response::new(ReportTaskOutcomeResponse {}));
        }
        let encoding = OutputEncoding::try_from(output_encoding)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let encoding_str = match encoding {
            OutputEncoding::Json => "application/json",
            OutputEncoding::Pickle => "application/octet-stream",
            OutputEncoding::Binary => "application/octet-stream",
            OutputEncoding::Unknown => "unknown",
        };
        let mut task = task.unwrap();
        match task_outcome {
            executor_api_pb::TaskOutcome::Success => {
                task.outcome = TaskOutcome::Success;
            }
            executor_api_pb::TaskOutcome::Failure => {
                task.outcome = TaskOutcome::Failure;
            }
            executor_api_pb::TaskOutcome::Unknown => {
                task.outcome = TaskOutcome::Unknown;
            }
        }
        task.output_status = TaskOutputsIngestionStatus::Ingested;
        task.status = TaskStatus::Completed;

        let mut node_outputs = Vec::new();
        for output in request.get_ref().fn_outputs.clone() {
            let path = output
                .path
                .ok_or(Status::invalid_argument("path is required"))?;
            let size = output
                .size
                .ok_or(Status::invalid_argument("size is required"))?;
            let sha256_hash = output
                .sha256_hash
                .ok_or(Status::invalid_argument("sha256_hash is required"))?;
            let data_payload = DataPayload {
                path,
                size,
                sha256_hash,
            };
            let node_output = NodeOutputBuilder::default()
                .namespace(namespace.to_string())
                .compute_graph_name(compute_graph.to_string())
                .invocation_id(invocation_id.to_string())
                .compute_fn_name(compute_fn.to_string())
                .payload(OutputPayload::Fn(data_payload))
                .encoding(encoding_str.to_string())
                .build()
                .map_err(|e| Status::internal(e.to_string()))?;
            node_outputs.push(node_output);
        }

        if request.get_ref().next_functions.len() > 0 {
            let node_output = NodeOutputBuilder::default()
                .namespace(namespace.to_string())
                .compute_graph_name(compute_graph.to_string())
                .invocation_id(invocation_id.to_string())
                .compute_fn_name(compute_fn.to_string())
                .payload(OutputPayload::Router(data_model::RouterOutput {
                    edges: request.get_ref().next_functions.clone(),
                }))
                .build()
                .map_err(|e| Status::internal(e.to_string()))?;
            node_outputs.push(node_output);
        }
        let task_diagnostic = TaskDiagnostics {
            stdout: prepare_data_payload(request.get_ref().stdout.clone()),
            stderr: prepare_data_payload(request.get_ref().stderr.clone()),
        };
        task.diagnostics = Some(task_diagnostic.clone());
        let request = RequestPayload::IngestTaskOutputs(IngestTaskOutputsRequest {
            namespace: namespace.to_string(),
            compute_graph: compute_graph.to_string(),
            compute_fn: compute_fn.to_string(),
            invocation_id: invocation_id.to_string(),
            task: task.clone(),
            node_outputs,
            executor_id: ExecutorId::new(executor_id.clone()),
        });

        let sm_req = StateMachineUpdateRequest {
            payload: request,
            processed_state_changes: vec![],
        };

        self.indexify_state
            .write(sm_req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ReportTaskOutcomeResponse {}))
    }
}

fn prepare_data_payload(
    msg: Option<executor_api_pb::DataPayload>,
) -> Option<data_model::DataPayload> {
    if msg.is_none() {
        return None;
    }
    if msg.as_ref().unwrap().path.as_ref().is_none() {
        return None;
    }
    if msg.as_ref().unwrap().size.as_ref().is_none() {
        return None;
    }
    if msg.as_ref().unwrap().sha256_hash.as_ref().is_none() {
        return None;
    }
    let msg = msg.unwrap();
    Some(data_model::DataPayload {
        path: msg.path.unwrap(),
        size: msg.size.unwrap(),
        sha256_hash: msg.sha256_hash.unwrap(),
    })
}
