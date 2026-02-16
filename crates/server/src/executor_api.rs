use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::Instant,
    vec,
};

use anyhow::Result;
use executor_api_pb::{
    Allocation,
    AllocationResult,
    AllowedFunction,
    DataPayload as DataPayloadPb,
    DataPayloadEncoding,
    DesiredExecutorState,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorResources,
    FunctionExecutorStatus,
    FunctionExecutorType as FunctionExecutorTypePb,
    GetDesiredExecutorStatesRequest,
    HostResources,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
    executor_api_server::ExecutorApi,
};
pub use proto_api::executor_api_pb;
use tokio::sync::watch::{self, Receiver, Sender};
use tokio_stream::{Stream, wrappers::WatchStream};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{
        self,
        ContainerBuilder,
        ContainerId,
        ContainerType,
        DataPayload,
        DataPayloadBuilder,
        ExecutorId,
        ExecutorMetadata,
        ExecutorMetadataBuilder,
        FunctionAllowlist,
        FunctionCallId,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        GPUResources,
    },
    executor_api::executor_api_pb::{FunctionExecutorState, FunctionExecutorTerminationReason},
    executors::ExecutorManager,
    metrics::GrpcMetrics,
    pb_helpers::blob_store_url_to_path,
    state_store::{
        IndexifyState,
        executor_watches::ExecutorWatch,
        requests::{
            AllocationOutput,
            FunctionCallRequest,
            RequestPayload,
            RequestUpdates,
            StateMachineUpdateRequest,
            UpsertExecutorRequest,
        },
    },
};

impl TryFrom<AllowedFunction> for FunctionAllowlist {
    type Error = anyhow::Error;

    fn try_from(allowed_function: AllowedFunction) -> Result<Self, Self::Error> {
        Ok(FunctionAllowlist {
            namespace: allowed_function.namespace,
            application: allowed_function.application_name,
            function: allowed_function.function_name,
        })
    }
}

impl TryFrom<data_model::HostResources> for HostResources {
    type Error = anyhow::Error;

    fn try_from(from: data_model::HostResources) -> Result<Self, Self::Error> {
        Ok(HostResources {
            // int division is okay because cpu_ms_per_sec derived from host hardware CPU cores is
            // always a multiple of 1000.
            cpu_count: Some(from.cpu_ms_per_sec / 1000),
            memory_bytes: Some(from.memory_bytes),
            disk_bytes: Some(from.disk_bytes),
            gpu: from.gpu.map(|g| g.try_into()).transpose()?,
        })
    }
}

impl TryFrom<HostResources> for data_model::HostResources {
    type Error = anyhow::Error;

    fn try_from(from: HostResources) -> Result<Self, Self::Error> {
        let cpu = from
            .cpu_count
            .ok_or(anyhow::anyhow!("cpu_count is required"))?;
        let memory = from
            .memory_bytes
            .ok_or(anyhow::anyhow!("memory_bytes is required"))?;
        let disk = from
            .disk_bytes
            .ok_or(anyhow::anyhow!("disk_bytes is required"))?;
        let gpu = from.gpu.map(|g| g.try_into()).transpose()?;
        Ok(data_model::HostResources {
            cpu_ms_per_sec: cpu * 1000,
            memory_bytes: memory,
            disk_bytes: disk,
            gpu,
        })
    }
}

impl From<ExecutorStatus> for data_model::ExecutorState {
    fn from(status: ExecutorStatus) -> Self {
        match status {
            ExecutorStatus::StartingUp => data_model::ExecutorState::StartingUp,
            ExecutorStatus::Running => data_model::ExecutorState::Running,
            ExecutorStatus::Drained => data_model::ExecutorState::Drained,
            ExecutorStatus::Stopped => data_model::ExecutorState::Stopped,
            ExecutorStatus::Unknown => data_model::ExecutorState::Unknown,
        }
    }
}

impl TryFrom<data_model::GPUResources> for executor_api_pb::GpuResources {
    type Error = anyhow::Error;

    fn try_from(gpu_resources: data_model::GPUResources) -> Result<Self, Self::Error> {
        if gpu_resources.count == 0 {
            return Err(anyhow::anyhow!("data_model gpu_resources.count is 0"));
        }
        let proto_model = match gpu_resources.model.as_str() {
            data_model::GPU_MODEL_NVIDIA_A100_40GB => Ok(executor_api_pb::GpuModel::NvidiaA10040gb),
            data_model::GPU_MODEL_NVIDIA_A100_80GB => Ok(executor_api_pb::GpuModel::NvidiaA10080gb),
            data_model::GPU_MODEL_NVIDIA_H100_80GB => Ok(executor_api_pb::GpuModel::NvidiaH10080gb),
            data_model::GPU_MODEL_NVIDIA_TESLA_T4 => Ok(executor_api_pb::GpuModel::NvidiaTeslaT4),
            data_model::GPU_MODEL_NVIDIA_A6000 => Ok(executor_api_pb::GpuModel::NvidiaA6000),
            data_model::GPU_MODEL_NVIDIA_A10 => Ok(executor_api_pb::GpuModel::NvidiaA10),
            _ => Err(anyhow::anyhow!("unknown data_model gpu_resources.model")),
        }?;
        Ok(executor_api_pb::GpuResources {
            count: Some(gpu_resources.count),
            model: Some(proto_model.into()),
        })
    }
}

impl TryFrom<executor_api_pb::GpuResources> for data_model::GPUResources {
    type Error = anyhow::Error;

    fn try_from(gpu_resources: executor_api_pb::GpuResources) -> Result<Self, Self::Error> {
        if gpu_resources.count() == 0 {
            return Err(anyhow::anyhow!("proto gpu_resources.count is 0"));
        }
        let str_model = match gpu_resources.model() {
            executor_api_pb::GpuModel::Unknown => {
                Err(anyhow::anyhow!("proto gpu_resources.model is unknown"))
            }
            executor_api_pb::GpuModel::NvidiaA10040gb => Ok(data_model::GPU_MODEL_NVIDIA_A100_40GB),
            executor_api_pb::GpuModel::NvidiaA10080gb => Ok(data_model::GPU_MODEL_NVIDIA_A100_80GB),
            executor_api_pb::GpuModel::NvidiaH10080gb => Ok(data_model::GPU_MODEL_NVIDIA_H100_80GB),
            executor_api_pb::GpuModel::NvidiaTeslaT4 => Ok(data_model::GPU_MODEL_NVIDIA_TESLA_T4),
            executor_api_pb::GpuModel::NvidiaA6000 => Ok(data_model::GPU_MODEL_NVIDIA_A6000),
            executor_api_pb::GpuModel::NvidiaA10 => Ok(data_model::GPU_MODEL_NVIDIA_A10),
        }?;
        Ok(data_model::GPUResources {
            count: gpu_resources.count(),
            model: str_model.into(),
        })
    }
}

impl TryFrom<FunctionExecutorResources> for data_model::ContainerResources {
    type Error = anyhow::Error;

    fn try_from(from: FunctionExecutorResources) -> Result<Self, Self::Error> {
        let cpu_ms_per_sec = from
            .cpu_ms_per_sec
            .ok_or(anyhow::anyhow!("cpu_ms_per_sec is required"))?;
        let memory_bytes = from
            .memory_bytes
            .ok_or(anyhow::anyhow!("memory_bytes is required"))?;
        let ephemeral_disk_bytes = from
            .disk_bytes
            .ok_or(anyhow::anyhow!("disk_bytes is required"))?;
        Ok(data_model::ContainerResources {
            cpu_ms_per_sec,
            // int division is okay because all the values were initially in MB and GB.
            memory_mb: (memory_bytes / 1024 / 1024) as u64,
            ephemeral_disk_mb: (ephemeral_disk_bytes / 1024 / 1024) as u64,
            gpu: from.gpu.map(GPUResources::try_from).transpose()?,
        })
    }
}

impl TryFrom<data_model::ContainerResources> for FunctionExecutorResources {
    type Error = anyhow::Error;

    fn try_from(from: data_model::ContainerResources) -> Result<Self, Self::Error> {
        Ok(FunctionExecutorResources {
            cpu_ms_per_sec: Some(from.cpu_ms_per_sec),
            memory_bytes: Some(from.memory_mb * 1024 * 1024),
            disk_bytes: Some(from.ephemeral_disk_mb * 1024 * 1024),
            gpu: from
                .gpu
                .map(|g| {
                    g.try_into()
                        .map_err(|e| anyhow::anyhow!("failed to convert GPU resources: {e}"))
                })
                .transpose()?,
        })
    }
}

impl TryFrom<ExecutorState> for ExecutorMetadata {
    type Error = anyhow::Error;

    fn try_from(executor_state: ExecutorState) -> Result<Self, Self::Error> {
        let mut executor_metadata = ExecutorMetadataBuilder::default();
        let executor_id = executor_state
            .executor_id
            .clone()
            .map(ExecutorId::new)
            .ok_or(anyhow::anyhow!("executor_id is required"))?;
        executor_metadata.catalog_name(executor_state.catalog_entry_name.clone());
        executor_metadata.id(executor_id.clone());
        executor_metadata.state(executor_state.status().into());
        if let Some(state_hash) = executor_state.state_hash.clone() {
            executor_metadata.state_hash(state_hash);
        }
        if let Some(executor_version) = executor_state.version {
            executor_metadata.executor_version(executor_version);
        }
        let mut allowed_functions = Vec::new();
        for function in executor_state.allowed_functions {
            allowed_functions.push(FunctionAllowlist::try_from(function)?);
        }
        if allowed_functions.is_empty() {
            executor_metadata.function_allowlist(None);
        } else {
            executor_metadata.function_allowlist(Some(allowed_functions));
        }
        if let Some(addr) = executor_state.hostname {
            executor_metadata.addr(addr);
        }
        executor_metadata.labels(executor_state.labels);
        let mut function_executors = HashMap::new();
        for function_executor_state in executor_state.function_executor_states {
            let function_executor = data_model::Container::try_from(function_executor_state)?;
            function_executors.insert(function_executor.id.clone(), function_executor);
        }
        executor_metadata.containers(function_executors);
        if let Some(host_resources) = executor_state.total_function_executor_resources {
            let cpu = host_resources
                .cpu_count
                .ok_or(anyhow::anyhow!("cpu_count is required"))?;
            let memory = host_resources
                .memory_bytes
                .ok_or(anyhow::anyhow!("memory_bytes is required"))?;
            let disk = host_resources
                .disk_bytes
                .ok_or(anyhow::anyhow!("disk_bytes is required"))?;
            // Ignore errors during conversion as they are expected e.g. if Executor GPU
            // model is unknown.
            let gpu = match host_resources.gpu {
                Some(gpu_resources) => gpu_resources.try_into().ok(),
                None => None,
            };
            executor_metadata.host_resources(data_model::HostResources {
                cpu_ms_per_sec: cpu * 1000,
                memory_bytes: memory,
                disk_bytes: disk,
                gpu,
            });
            executor_metadata.host_resources(host_resources.try_into()?);
        }
        if let Some(server_clock) = executor_state.server_clock {
            executor_metadata.clock(server_clock);
        }
        if let Some(tls_proxy_address) = executor_state.proxy_address {
            executor_metadata.proxy_address(Some(tls_proxy_address));
        }
        executor_metadata.build().map_err(Into::into)
    }
}

impl TryFrom<FunctionExecutorTerminationReason> for data_model::FunctionExecutorTerminationReason {
    type Error = anyhow::Error;

    fn try_from(
        termination_reason: FunctionExecutorTerminationReason,
    ) -> Result<Self, Self::Error> {
        match termination_reason {
            FunctionExecutorTerminationReason::Unknown => {
                Ok(data_model::FunctionExecutorTerminationReason::Unknown)
            }
            FunctionExecutorTerminationReason::StartupFailedInternalError => {
                Ok(data_model::FunctionExecutorTerminationReason::StartupFailedInternalError)
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionError => {
                Ok(data_model::FunctionExecutorTerminationReason::StartupFailedFunctionError)
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionTimeout => {
                Ok(data_model::FunctionExecutorTerminationReason::StartupFailedFunctionTimeout)
            }
            FunctionExecutorTerminationReason::Unhealthy => {
                Ok(data_model::FunctionExecutorTerminationReason::Unhealthy)
            }
            FunctionExecutorTerminationReason::InternalError => {
                Ok(data_model::FunctionExecutorTerminationReason::InternalError)
            }
            FunctionExecutorTerminationReason::FunctionTimeout => {
                Ok(data_model::FunctionExecutorTerminationReason::FunctionTimeout)
            }
            FunctionExecutorTerminationReason::FunctionCancelled => {
                Ok(data_model::FunctionExecutorTerminationReason::FunctionCancelled)
            }
            FunctionExecutorTerminationReason::Oom => {
                Ok(data_model::FunctionExecutorTerminationReason::Oom)
            }
            FunctionExecutorTerminationReason::ProcessCrash => {
                Ok(data_model::FunctionExecutorTerminationReason::ProcessCrash)
            }
        }
    }
}

impl TryFrom<FunctionExecutorState> for data_model::Container {
    type Error = anyhow::Error;

    fn try_from(function_executor_state: FunctionExecutorState) -> Result<Self, Self::Error> {
        let termination_reason = data_model::FunctionExecutorTerminationReason::try_from(
            function_executor_state.termination_reason(),
        )?;
        let id = function_executor_state
            .description
            .as_ref()
            .and_then(|description| description.id.clone())
            .ok_or(anyhow::anyhow!("id is required"))?;
        let function_ref = function_executor_state
            .description
            .as_ref()
            .and_then(|description| description.function.clone())
            .ok_or(anyhow::anyhow!("function ref is required"))?;
        let namespace = function_ref
            .namespace
            .clone()
            .ok_or(anyhow::anyhow!("namespace is required"))?;
        let application_name = function_ref
            .application_name
            .clone()
            .ok_or(anyhow::anyhow!("application_name is required"))?;
        let function_name = function_ref
            .function_name
            .clone()
            .ok_or(anyhow::anyhow!("function_name is required"))?;
        let version = function_ref
            .application_version
            .clone()
            .ok_or(anyhow::anyhow!("application_version is required"))?;
        let resources = function_executor_state
            .description
            .as_ref()
            .and_then(|description| description.resources)
            .ok_or(anyhow::anyhow!("resources is required"))?;
        let resources = data_model::ContainerResources::try_from(resources)?;
        let max_concurrency = function_executor_state
            .description
            .as_ref()
            .and_then(|description| description.max_concurrency)
            .unwrap_or(1);
        // TODO: uncomment this once Executor gets deployed and provides this.
        // .ok_or(anyhow::anyhow!("max_concurrency is required"))?;

        let description = function_executor_state.description.as_ref();

        // Get container_type from description (moved from FunctionExecutorState)
        let container_type = description
            .map(|d| match d.container_type() {
                FunctionExecutorTypePb::Unknown => ContainerType::Function, // Default for backwards compat
                FunctionExecutorTypePb::Function => ContainerType::Function,
                FunctionExecutorTypePb::Sandbox => ContainerType::Sandbox,
            })
            .unwrap_or(ContainerType::Function);

        let secret_names = description
            .map(|d| d.secret_names.clone())
            .unwrap_or_default();

        // Read sandbox-specific fields from sandbox_metadata
        let sandbox_metadata = description.and_then(|d| d.sandbox_metadata.as_ref());
        let timeout_secs = sandbox_metadata.and_then(|m| m.timeout_secs).unwrap_or(0);
        let entrypoint = sandbox_metadata
            .map(|m| m.entrypoint.clone())
            .unwrap_or_default();
        let image = sandbox_metadata.and_then(|m| m.image.clone());
        let sandbox_id = sandbox_metadata
            .and_then(|m| m.sandbox_id.clone())
            .map(data_model::SandboxId::new);

        let state = match function_executor_state.status() {
            FunctionExecutorStatus::Unknown => data_model::ContainerState::Unknown,
            FunctionExecutorStatus::Pending => data_model::ContainerState::Pending,
            FunctionExecutorStatus::Running => data_model::ContainerState::Running,
            FunctionExecutorStatus::Terminated => data_model::ContainerState::Terminated {
                reason: termination_reason,
                failed_alloc_ids: function_executor_state.allocation_ids_caused_termination,
            },
        };

        // Get pool_id from protobuf, or compute from identity as fallback for backwards
        // compat FIXME - Remove this once all executors have been updated to
        // set pool_id.
        let pool_id = description
            .and_then(|d| d.pool_id.clone())
            .map(data_model::ContainerPoolId::new);
        // Fallback for old executors that don't set pool_id:
        let pool_id = pool_id.or_else(|| match container_type {
            ContainerType::Function => Some(data_model::ContainerPoolId::for_function(
                &application_name,
                &function_name,
                &version,
            )),
            ContainerType::Sandbox => None, // Standalone sandboxes have no pool
        });

        ContainerBuilder::default()
            .id(ContainerId::new(id.clone()))
            .namespace(namespace.clone())
            .application_name(application_name.clone())
            .function_name(function_name.clone())
            .version(version.clone())
            .state(state)
            .resources(resources)
            .max_concurrency(max_concurrency)
            .container_type(container_type)
            .secret_names(secret_names)
            .timeout_secs(timeout_secs)
            .entrypoint(entrypoint)
            .image(image)
            .pool_id(pool_id)
            .sandbox_id(sandbox_id)
            .build()
            .map_err(Into::into)
    }
}

fn to_internal_function_arg(
    function_arg: executor_api_pb::FunctionArg,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> Result<data_model::FunctionArgs, anyhow::Error> {
    let source = function_arg
        .source
        .ok_or(anyhow::anyhow!("source is required"))?;
    match source {
        executor_api_pb::function_arg::Source::FunctionCallId(function_call_id) => Ok(
            data_model::FunctionArgs::FunctionRunOutput(FunctionCallId(function_call_id)),
        ),
        executor_api_pb::function_arg::Source::InlineData(inline_data) => {
            Ok(data_model::FunctionArgs::DataPayload(prepare_data_payload(
                inline_data,
                blob_store_url_scheme,
                blob_store_url,
            )?))
        }
    }
}

fn to_internal_function_call(
    function_call: executor_api_pb::FunctionCall,
    blob_storage_registry: &BlobStorageRegistry,
    source_function_call_id: Option<String>,
) -> Result<data_model::FunctionCall, anyhow::Error> {
    let target = function_call
        .target
        .ok_or(anyhow::anyhow!("target is required"))?;
    let namespace = target
        .namespace
        .ok_or(anyhow::anyhow!("namespace is required"))?;
    let blob_storage_url_scheme = blob_storage_registry
        .get_blob_store(&namespace)
        .get_url_scheme();
    let blob_storage_url = blob_storage_registry.get_blob_store(&namespace).get_url();
    Ok(data_model::FunctionCall {
        function_call_id: FunctionCallId(
            function_call.id.ok_or(anyhow::anyhow!("id is required"))?,
        ),
        fn_name: target
            .function_name
            .ok_or(anyhow::anyhow!("function_name is required"))?,
        inputs: function_call
            .args
            .into_iter()
            .map(|arg| to_internal_function_arg(arg, &blob_storage_url_scheme, &blob_storage_url))
            .collect::<Result<Vec<data_model::FunctionArgs>, anyhow::Error>>()?,
        call_metadata: function_call.call_metadata.unwrap_or_default().into(),
        parent_function_call_id: source_function_call_id.map(FunctionCallId::from),
    })
}

fn to_internal_reduce_op(
    reduce_op: executor_api_pb::ReduceOp,
    blob_storage_registry: &BlobStorageRegistry,
) -> Result<data_model::ReduceOperation> {
    let reducer = reduce_op
        .reducer
        .ok_or(anyhow::anyhow!("reducer is required"))?;
    let namespace = reducer
        .namespace
        .ok_or(anyhow::anyhow!("namespace is required"))?;
    let blob_storage_url_scheme = blob_storage_registry
        .get_blob_store(&namespace)
        .get_url_scheme();
    let blob_storage_url = blob_storage_registry.get_blob_store(&namespace).get_url();
    Ok(data_model::ReduceOperation {
        function_call_id: FunctionCallId(
            reduce_op
                .id
                .ok_or(anyhow::anyhow!("reduce op id is required"))?,
        ),
        fn_name: reducer
            .function_name
            .ok_or(anyhow::anyhow!("function_name is required"))?,
        call_metadata: reduce_op
            .call_metadata
            .ok_or(anyhow::anyhow!("call_metadata is required"))?
            .into(),
        collection: reduce_op
            .collection
            .into_iter()
            .map(|arg| to_internal_function_arg(arg, &blob_storage_url_scheme, &blob_storage_url))
            .collect::<Result<Vec<data_model::FunctionArgs>, anyhow::Error>>()?,
    })
}

fn to_internal_compute_op(
    compute_op: executor_api_pb::ExecutionPlanUpdate,
    blob_storage_registry: &BlobStorageRegistry,
    source_function_call_id: Option<String>,
) -> Result<data_model::ComputeOp, anyhow::Error> {
    let op = compute_op.op.ok_or(anyhow::anyhow!("op is required"))?;
    match op {
        executor_api_pb::execution_plan_update::Op::FunctionCall(function_call) => Ok(
            data_model::ComputeOp::FunctionCall(to_internal_function_call(
                function_call,
                blob_storage_registry,
                source_function_call_id,
            )?),
        ),
        executor_api_pb::execution_plan_update::Op::Reduce(reduce) => Ok(
            data_model::ComputeOp::Reduce(to_internal_reduce_op(reduce, blob_storage_registry)?),
        ),
    }
}

impl TryFrom<&executor_api_pb::FunctionCallWatch> for ExecutorWatch {
    type Error = anyhow::Error;

    fn try_from(value: &executor_api_pb::FunctionCallWatch) -> Result<Self> {
        let namespace = value
            .namespace
            .as_ref()
            .ok_or(anyhow::anyhow!("Missing namespace in FunctionCallWatch"))?
            .clone();
        let application = value
            .application
            .as_ref()
            .ok_or(anyhow::anyhow!("Missing application in FunctionCallWatch"))?
            .clone();
        let request_id = value
            .request_id
            .as_ref()
            .ok_or(anyhow::anyhow!("Missing request_id in FunctionCallWatch"))?
            .clone();
        let function_call_id = value
            .function_call_id
            .as_ref()
            .ok_or(anyhow::anyhow!(
                "Missing function_call_id in FunctionCallWatch"
            ))?
            .clone();
        Ok(ExecutorWatch {
            namespace,
            application,
            request_id,
            function_call_id,
        })
    }
}

pub struct ExecutorAPIService {
    indexify_state: Arc<IndexifyState>,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
    grpc_metrics: GrpcMetrics,
}

impl ExecutorAPIService {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        executor_manager: Arc<ExecutorManager>,
        blob_storage_registry: Arc<BlobStorageRegistry>,
    ) -> Self {
        Self {
            indexify_state,
            executor_manager,
            blob_storage_registry,
            grpc_metrics: GrpcMetrics::new(),
        }
    }

    async fn handle_allocation_results(
        &self,
        executor_id: ExecutorId,
        alloc_results: Vec<AllocationResult>,
    ) -> Result<Vec<AllocationOutput>> {
        if !alloc_results.is_empty() {
            info!(
                executor_id = executor_id.get(),
                num_results = alloc_results.len(),
                results = ?alloc_results.iter().map(|r| format!("{}:{}:{}", r.function.as_ref().map(|f| f.function_name()).unwrap_or_default(), r.allocation_id(), r.request_id())).collect::<Vec<_>>(),
                "handling allocation results from executor"
            );
        }
        let mut allocation_output_updates = Vec::new();
        for alloc_result in alloc_results {
            let function_ref = alloc_result
                .function
                .as_ref()
                .ok_or(anyhow::anyhow!("function ref is required"))?;
            let allocation_key = data_model::Allocation::key_from(
                function_ref.namespace(),
                function_ref.application_name(),
                alloc_result.request_id(),
                alloc_result.allocation_id(),
            );
            let Some(mut allocation) = self
                .indexify_state
                .reader()
                .get_allocation(&allocation_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            else {
                warn!(
                    allocation_key = %allocation_key,
                    request_id = %alloc_result.request_id(),
                    fn_name = %function_ref.function_name(),
                    "allocation not found, skipping this result (was likely already processed or cancelled)"
                );
                continue; // Don't drop ALL results, just skip this one
            };
            let outcome_code = executor_api_pb::AllocationOutcomeCode::try_from(
                alloc_result.outcome_code.unwrap_or(0),
            )
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let failure_reason = alloc_result
                .failure_reason
                .map(|reason| {
                    executor_api_pb::AllocationFailureReason::try_from(reason)
                        .map_err(|e| Status::invalid_argument(e.to_string()))
                })
                .transpose()?;
            let execution_duration_ms = alloc_result.execution_duration_ms.unwrap_or(0);

            let blob_storage_url_schema = self
                .blob_storage_registry
                .get_blob_store(&allocation.namespace)
                .get_url_scheme();
            let blob_storage_url = self
                .blob_storage_registry
                .get_blob_store(&allocation.namespace)
                .get_url();
            let mut fn_output: Option<DataPayload> = None;
            let mut request_updates: Option<RequestUpdates> = None;
            if let Some(return_value) = alloc_result.return_value.clone() {
                match return_value {
                    executor_api_pb::allocation_result::ReturnValue::Value(value) => {
                        fn_output = Some(prepare_data_payload(
                            value,
                            &blob_storage_url_schema,
                            &blob_storage_url,
                        )?);
                    }
                    executor_api_pb::allocation_result::ReturnValue::Updates(updates) => {
                        let output_function_call_id = FunctionCallId(
                            updates
                                .root_function_call_id
                                .ok_or(anyhow::anyhow!("root function call id is required"))?,
                        );
                        let mut function_calls = Vec::new();
                        for update in updates.updates {
                            function_calls.push(to_internal_compute_op(
                                update,
                                &self.blob_storage_registry,
                                None,
                            )?);
                        }
                        request_updates = Some(RequestUpdates {
                            request_updates: function_calls,
                            output_function_call_id,
                        });
                    }
                }
            }

            let request_error_payload = match alloc_result.request_error.clone() {
                Some(exception) => Some(prepare_data_payload(
                    exception,
                    &blob_storage_url_schema,
                    &blob_storage_url,
                )?),
                None => None,
            };

            let allocation_failure_reason = match failure_reason {
                Some(reason) => match reason {
                    executor_api_pb::AllocationFailureReason::Unknown => {
                        Some(FunctionRunFailureReason::Unknown)
                    }
                    executor_api_pb::AllocationFailureReason::InternalError => {
                        Some(FunctionRunFailureReason::InternalError)
                    }
                    executor_api_pb::AllocationFailureReason::FunctionError => {
                        Some(FunctionRunFailureReason::FunctionError)
                    }
                    executor_api_pb::AllocationFailureReason::FunctionTimeout => {
                        Some(FunctionRunFailureReason::FunctionTimeout)
                    }
                    executor_api_pb::AllocationFailureReason::RequestError => {
                        Some(FunctionRunFailureReason::RequestError)
                    }
                    executor_api_pb::AllocationFailureReason::AllocationCancelled => {
                        Some(FunctionRunFailureReason::FunctionRunCancelled)
                    }
                    executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated => {
                        Some(FunctionRunFailureReason::FunctionExecutorTerminated)
                    }
                    executor_api_pb::AllocationFailureReason::Oom => {
                        Some(FunctionRunFailureReason::OutOfMemory)
                    }
                    executor_api_pb::AllocationFailureReason::ConstraintUnsatisfiable => {
                        Some(FunctionRunFailureReason::ConstraintUnsatisfiable)
                    }
                    executor_api_pb::AllocationFailureReason::ExecutorRemoved => {
                        Some(FunctionRunFailureReason::ExecutorRemoved)
                    }
                    executor_api_pb::AllocationFailureReason::StartupFailedInternalError => {
                        Some(FunctionRunFailureReason::ContainerStartupInternalError)
                    }
                    executor_api_pb::AllocationFailureReason::StartupFailedFunctionError => {
                        Some(FunctionRunFailureReason::ContainerStartupFunctionError)
                    }
                    executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout => {
                        Some(FunctionRunFailureReason::ContainerStartupFunctionTimeout)
                    }
                },
                None => None,
            };
            let task_outcome = match outcome_code {
                executor_api_pb::AllocationOutcomeCode::Success => FunctionRunOutcome::Success,
                executor_api_pb::AllocationOutcomeCode::Failure => {
                    let failure_reason =
                        allocation_failure_reason.unwrap_or(FunctionRunFailureReason::Unknown);
                    FunctionRunOutcome::Failure(failure_reason)
                }
                executor_api_pb::AllocationOutcomeCode::Unknown => FunctionRunOutcome::Unknown,
            };
            allocation.outcome = task_outcome;
            allocation.execution_duration_ms = Some(execution_duration_ms);

            let request = AllocationOutput {
                request_exception: request_error_payload,
                request_id: alloc_result.request_id().to_string(),
                executor_id: executor_id.clone(),
                allocation,
                data_payload: fn_output,
                graph_updates: request_updates,
            };
            allocation_output_updates.push(request);
        }
        Ok(allocation_output_updates)
    }
}

fn log_desired_executor_state_delta(
    last_sent_state: &DesiredExecutorState,
    desired_state: &DesiredExecutorState,
) {
    debug!(?desired_state, "got desired state");

    let mut last_assignments: HashMap<String, String> = HashMap::default();
    for ta in &last_sent_state.allocations {
        if let (Some(fe_id), Some(alloc_id)) = (&ta.function_executor_id, &ta.allocation_id) {
            last_assignments.insert(fe_id.clone(), alloc_id.clone());
        }
    }

    for Allocation {
        function_executor_id: fn_executor_id_option,
        allocation_id: allocation_id_option,
        ..
    } in &desired_state.allocations
    {
        if let (Some(fn_executor_id), Some(allocation_id)) =
            (fn_executor_id_option, allocation_id_option)
        {
            match last_assignments.get(fn_executor_id) {
                Some(last_allocation_id) => {
                    if allocation_id != last_allocation_id {
                        debug!(
                            %fn_executor_id,
                            %allocation_id, %last_allocation_id, "re-assigning FE"
                        )
                    }
                    last_assignments.remove(fn_executor_id);
                }
                None => {
                    debug!(%fn_executor_id, %allocation_id, "assigning FE")
                }
            }
        }
    }

    for (fn_executor_id, last_allocation_id) in last_assignments {
        debug!(%fn_executor_id, %last_allocation_id, "idling FE")
    }
}

#[instrument(skip_all, fields(executor_id = %executor_id))]
async fn executor_update_loop(
    executor_id: ExecutorId,
    executor_manager: Arc<ExecutorManager>,
    mut executor_state_rx: Receiver<()>,
    grpc_tx: Sender<Result<DesiredExecutorState, Status>>,
) {
    // Mark the state as changed to trigger the first change
    // notification to the executor. This is important because between
    // the report_executor_state and the get_desired_executor_states
    // requests, the executor state may received a new desired state.
    executor_state_rx.mark_changed();

    // Use the default/empty value for the last-sent desired state, so
    // that the first change logged will be a delta from nothing (=>
    // the complete executor state).
    let mut last_sent_state = DesiredExecutorState::default();

    loop {
        // Wait for a state change for this executor or grpc stream closing.
        tokio::select! {
            _ = grpc_tx.closed() => {
                info!(
                    "get_desired_executor_states: grpc stream closed"
                );
                break;
            }
            result = executor_state_rx.changed() => {
                if let Err(err) = result {
                    info!(
                        ?err,
                        "state machine watcher closing"
                    );
                    break;
                }
            }
        }

        let Some(desired_state) = executor_manager.get_executor_state(&executor_id).await else {
            debug!(
                executor_id = executor_id.get(),
                "executor not ready, waiting for reconciliation"
            );
            continue;
        };

        // Log the state delta
        log_desired_executor_state_delta(&last_sent_state, &desired_state);

        // Send the state to the executor
        if let Err(err) = grpc_tx.send(Ok(desired_state.clone())) {
            info!(?err, "grpc stream closing");
            break;
        }

        // Store the sent state for next comparison
        last_sent_state = desired_state;
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
        let start = Instant::now();
        let _timer = self.grpc_metrics.record_request("report_executor_state");

        let executor_state = request
            .get_ref()
            .executor_state
            .clone()
            .ok_or(Status::invalid_argument("executor_state is required"))?;
        let executor_id = executor_state
            .executor_id
            .clone()
            .ok_or(Status::invalid_argument("executor_id is required"))?;
        let executor_id = ExecutorId::new(executor_id);
        let executor_update = request
            .get_ref()
            .executor_update
            .clone()
            .ok_or(Status::invalid_argument("executor_update is required"))?;

        let mut watch_function_calls = HashSet::new();
        for function_call_watch in &executor_state.function_call_watches {
            let executor_watch: ExecutorWatch = function_call_watch.try_into().map_err(|e| {
                Status::invalid_argument(format!("Invalid function call watch: {}", e))
            })?;
            watch_function_calls.insert(executor_watch);
        }

        if !watch_function_calls.is_empty() {
            info!(
                executor_id = executor_id.get(),
                num_watches = watch_function_calls.len(),
                watches = ?watch_function_calls.iter().map(|w| format!("{}:{}", w.function_call_id, w.request_id)).collect::<Vec<_>>(),
                "executor reported function call watches"
            );
        }

        trace!(
            executor_id = executor_id.get(),
            "Got report_executor_state request"
        );

        let executor_metadata = ExecutorMetadata::try_from(executor_state)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let update_executor_state = self
            .executor_manager
            .heartbeat(&executor_metadata)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let task_results = executor_update.allocation_results.clone();
        let allocation_outputs = self
            .handle_allocation_results(executor_id.clone(), task_results)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Don't return early if there are watches - they need to be synced
        if !update_executor_state &&
            allocation_outputs.is_empty() &&
            watch_function_calls.is_empty()
        {
            return Ok(Response::new(ReportExecutorStateResponse {}));
        }

        let request = UpsertExecutorRequest::build(
            executor_metadata,
            allocation_outputs,
            update_executor_state,
            watch_function_calls,
            self.indexify_state.clone(),
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::UpsertExecutor(request),
        };
        if let Err(e) = self.indexify_state.write(sm_req).await {
            error!(
                executor_id = executor_id.get(),
                "failed to write state machine update request: {e:?}"
            );
            return Err(Status::internal(
                "failed to write state machine update request",
            ));
        }

        let duration_sec = start.elapsed().as_secs_f64();
        if duration_sec >= 1.0 {
            warn!(
                executor_id = executor_id.get(),
                duration_sec = duration_sec,
                "report_executor_state took too long"
            );
        }

        Ok(Response::new(ReportExecutorStateResponse {}))
    }

    async fn get_desired_executor_states(
        &self,
        request: Request<GetDesiredExecutorStatesRequest>,
    ) -> Result<Response<Self::get_desired_executor_statesStream>, Status> {
        let _timer = self
            .grpc_metrics
            .record_request("get_desired_executor_states");

        let executor_id = request
            .get_ref()
            .executor_id
            .clone()
            .ok_or(Status::invalid_argument("executor_id is required"))?;
        let executor_id = ExecutorId::new(executor_id);

        trace!(
            executor_id = executor_id.get(),
            "Got get_desired_executor_states request",
        );

        let executor_state_rx = match self.executor_manager.subscribe(&executor_id).await {
            Some(state_rx) => state_rx,
            None => {
                let msg = "executor not found, or not yet registered";
                warn!(
                    executor_id = executor_id.get(),
                    "get_desired_executor_states: {}", msg
                );
                return Err(Status::not_found(msg));
            }
        };

        let (grpc_tx, grpc_rx) = watch::channel(Result::Ok(DesiredExecutorState {
            function_executors: vec![],
            allocations: vec![],
            clock: Some(0),
            function_call_results: vec![],
        }));
        tokio::spawn(executor_update_loop(
            executor_id,
            self.executor_manager.clone(),
            executor_state_rx,
            grpc_tx,
        ));

        let grpc_stream = WatchStream::from_changes(grpc_rx);
        Ok(Response::new(
            Box::pin(grpc_stream) as Self::get_desired_executor_statesStream
        ))
    }

    async fn call_function(
        &self,
        request: Request<executor_api_pb::FunctionCallRequest>,
    ) -> Result<Response<executor_api_pb::FunctionCallResponse>, Status> {
        let _timer = self.grpc_metrics.record_request("call_function");

        let req = request.into_inner();
        let updates = req
            .updates
            .ok_or(Status::invalid_argument("updates is required"))?;
        let request_id = req
            .request_id
            .ok_or(Status::invalid_argument("request_id is required"))?;
        let source_function_call_id = req.source_function_call_id.ok_or(
            Status::invalid_argument("source_function_call_id is required"),
        )?;
        let namespace = req
            .namespace
            .ok_or(Status::invalid_argument("namespace is required"))?;
        let application_name = req
            .application
            .ok_or(Status::invalid_argument("application is required"))?;
        let root_function_call_id =
            updates
                .root_function_call_id
                .ok_or(Status::invalid_argument(
                    "root function call id is required",
                ))?;
        let mut compute_ops = Vec::new();
        for update in updates.updates {
            compute_ops.push(
                to_internal_compute_op(
                    update,
                    &self.blob_storage_registry,
                    Some(source_function_call_id.clone()),
                )
                .map_err(|e| Status::internal(e.to_string()))?,
            );
        }
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::CreateFunctionCall(FunctionCallRequest {
                namespace,
                application_name,
                request_id,
                graph_updates: RequestUpdates {
                    request_updates: compute_ops,
                    output_function_call_id: FunctionCallId(root_function_call_id),
                },
                source_function_call_id: FunctionCallId(source_function_call_id),
            }),
        };
        if let Err(e) = self.indexify_state.write(sm_req).await {
            error!("failed to write state machine update request: {e:?}");
            return Err(Status::internal(
                "failed to write state machine update request",
            ));
        }
        Ok(Response::new(executor_api_pb::FunctionCallResponse {}))
    }
}

fn prepare_data_payload(
    msg: DataPayloadPb,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> Result<DataPayload> {
    let uri = msg.uri.ok_or(anyhow::anyhow!("uri is required"))?;
    let size = msg.size.ok_or(anyhow::anyhow!("size is required"))?;
    let sha256_hash = msg
        .sha256_hash
        .ok_or(anyhow::anyhow!("sha256_hash is required"))?;
    let output_encoding = msg
        .encoding
        .ok_or(anyhow::anyhow!("encoding is required"))?;
    let output_encoding = DataPayloadEncoding::try_from(output_encoding)?;
    let mut encoding = match output_encoding {
        DataPayloadEncoding::Utf8Json => "application/json",
        DataPayloadEncoding::BinaryPickle => "application/python-pickle",
        DataPayloadEncoding::Utf8Text => "text/plain",
        DataPayloadEncoding::BinaryZip => "application/zip",
        DataPayloadEncoding::Raw => "application/octet-stream",
        DataPayloadEncoding::Unknown => "application/octet-stream",
    };
    if let Some(content_type) = msg.content_type.as_ref() {
        // User supplied content type when a function returns tensorlake.File.
        // FIXME - The executor shouldn't set a content type as empty string
        // Update: this is fixed but Executor is not deployed yet.
        if !content_type.is_empty() {
            encoding = content_type.as_str();
        }
    }
    let metadata_size = msg.metadata_size.unwrap_or(0);
    let offset = msg.offset.unwrap_or(0);
    DataPayloadBuilder::default()
        .id(msg.id.unwrap_or(nanoid::nanoid!()))
        .path(blob_store_url_to_path(
            &uri,
            blob_store_url_scheme,
            blob_store_url,
        ))
        .encoding(encoding.to_string())
        .size(size)
        .sha256_hash(sha256_hash)
        .metadata_size(metadata_size)
        .offset(offset)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build data payload: {e}"))
}

#[cfg(test)]
mod tests {
    use executor_api_pb::{
        FunctionExecutorDescription,
        FunctionExecutorResources,
        FunctionExecutorState,
        FunctionExecutorStatus,
        FunctionExecutorTerminationReason as TerminationReasonPb,
        FunctionExecutorType as FunctionExecutorTypePb,
        FunctionRef,
        SandboxMetadata,
    };
    use proto_api::executor_api_pb;

    use crate::data_model::{self, ContainerType, FunctionExecutorTerminationReason};

    /// Build a minimal sandbox FunctionExecutorState proto with the given
    /// sandbox_id and termination status, simulating what the dataplane reports
    /// when a sandbox container fails to start (e.g. image pull failure).
    fn sandbox_fe_state_proto(
        container_id: &str,
        sandbox_id: &str,
        status: FunctionExecutorStatus,
        termination_reason: Option<TerminationReasonPb>,
    ) -> FunctionExecutorState {
        FunctionExecutorState {
            description: Some(FunctionExecutorDescription {
                id: Some(container_id.to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("".to_string()),
                    function_name: Some(container_id.to_string()),
                    application_version: Some("".to_string()),
                }),
                resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(FunctionExecutorTypePb::Sandbox.into()),
                sandbox_metadata: Some(SandboxMetadata {
                    image: Some("ubuntu".to_string()),
                    timeout_secs: Some(600),
                    entrypoint: vec![],
                    network_policy: None,
                    sandbox_id: Some(sandbox_id.to_string()),
                }),
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
            allocation_ids_caused_termination: vec![],
        }
    }

    #[test]
    fn test_terminated_sandbox_container_preserves_sandbox_id() {
        // Simulates the dataplane reporting a sandbox container that failed to
        // start (e.g. Docker image pull failure). The proto->Container conversion
        // must preserve sandbox_id so the reconciler can find and terminate the
        // associated sandbox.
        let fe_state = sandbox_fe_state_proto(
            "sb-container-123",
            "sb-container-123",
            FunctionExecutorStatus::Terminated,
            Some(TerminationReasonPb::StartupFailedInternalError),
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Sandbox);
        assert!(
            matches!(
                container.state,
                data_model::ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::StartupFailedInternalError,
                    ..
                }
            ),
            "Container should be Terminated with StartupFailedInternalError, got: {:?}",
            container.state
        );
        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-123"),
            "sandbox_id must be preserved through proto conversion"
        );
    }

    #[test]
    fn test_running_sandbox_container_preserves_sandbox_id() {
        let fe_state = sandbox_fe_state_proto(
            "sb-container-456",
            "sb-container-456",
            FunctionExecutorStatus::Running,
            None,
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-456"),
        );
        assert_eq!(container.state, data_model::ContainerState::Running);
    }

    #[test]
    fn test_function_container_has_no_sandbox_id() {
        // Function containers don't have sandbox_metadata, so sandbox_id should
        // be None.
        let fe_state = FunctionExecutorState {
            description: Some(FunctionExecutorDescription {
                id: Some("fn-container-1".to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("app".to_string()),
                    function_name: Some("process".to_string()),
                    application_version: Some("v1".to_string()),
                }),
                resources: Some(FunctionExecutorResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(FunctionExecutorTypePb::Function.into()),
                sandbox_metadata: None,
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(FunctionExecutorStatus::Running.into()),
            termination_reason: None,
            allocation_ids_caused_termination: vec![],
        };

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Function);
        assert!(
            container.sandbox_id.is_none(),
            "Function containers should not have sandbox_id"
        );
    }
}
