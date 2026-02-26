//! Proto ↔ internal data-model conversions.
//!
//! Pure mapping code extracted from `executor_api.rs` to keep the RPC
//! handler module focused on protocol logic.

use std::collections::HashMap;

use anyhow::Result;
use executor_api_pb::{
    AllowedFunction,
    ContainerResources,
    ContainerStatus,
    ContainerType as ContainerTypePb,
    DataPayload as DataPayloadPb,
    DataPayloadEncoding,
    ExecutorState,
    ExecutorStatus,
    HostResources,
};
pub use proto_api::executor_api_pb;

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{
        self,
        ContainerBuilder,
        ContainerType,
        DataPayload,
        DataPayloadBuilder,
        ExecutorId,
        ExecutorMetadata,
        ExecutorMetadataBuilder,
        FunctionAllowlist,
        FunctionCallId,
        GPUResources,
    },
    executor_api::executor_api_pb::{ContainerState, ContainerTerminationReason},
    pb_helpers::blob_store_url_to_path,
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
            ExecutorStatus::SchedulingDisabled => data_model::ExecutorState::SchedulingDisabled,
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

impl TryFrom<ContainerResources> for data_model::ContainerResources {
    type Error = anyhow::Error;

    fn try_from(from: ContainerResources) -> Result<Self, Self::Error> {
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

impl TryFrom<data_model::ContainerResources> for ContainerResources {
    type Error = anyhow::Error;

    fn try_from(from: data_model::ContainerResources) -> Result<Self, Self::Error> {
        Ok(ContainerResources {
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
        for function_executor_state in executor_state.container_states {
            let function_executor = data_model::Container::try_from(function_executor_state)?;
            function_executors.insert(function_executor.id.clone(), function_executor);
        }
        executor_metadata.containers(function_executors);
        if let Some(host_resources) = executor_state.total_container_resources {
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

impl TryFrom<ContainerTerminationReason> for data_model::ContainerTerminationReason {
    type Error = anyhow::Error;

    fn try_from(termination_reason: ContainerTerminationReason) -> Result<Self, Self::Error> {
        match termination_reason {
            ContainerTerminationReason::Unknown => {
                Ok(data_model::ContainerTerminationReason::Unknown)
            }
            ContainerTerminationReason::StartupFailedInternalError => {
                Ok(data_model::ContainerTerminationReason::StartupFailedInternalError)
            }
            ContainerTerminationReason::StartupFailedFunctionError => {
                Ok(data_model::ContainerTerminationReason::StartupFailedFunctionError)
            }
            ContainerTerminationReason::StartupFailedFunctionTimeout => {
                Ok(data_model::ContainerTerminationReason::StartupFailedFunctionTimeout)
            }
            ContainerTerminationReason::Unhealthy => {
                Ok(data_model::ContainerTerminationReason::Unhealthy)
            }
            ContainerTerminationReason::InternalError => {
                Ok(data_model::ContainerTerminationReason::InternalError)
            }
            ContainerTerminationReason::FunctionTimeout => {
                Ok(data_model::ContainerTerminationReason::FunctionTimeout)
            }
            ContainerTerminationReason::FunctionCancelled => {
                Ok(data_model::ContainerTerminationReason::FunctionCancelled)
            }
            ContainerTerminationReason::Oom => Ok(data_model::ContainerTerminationReason::Oom),
            ContainerTerminationReason::ProcessCrash => {
                Ok(data_model::ContainerTerminationReason::ProcessCrash)
            }
            ContainerTerminationReason::StartupFailedBadImage => {
                Ok(data_model::ContainerTerminationReason::StartupFailedBadImage)
            }
        }
    }
}

impl TryFrom<ContainerState> for data_model::Container {
    type Error = anyhow::Error;

    fn try_from(function_executor_state: ContainerState) -> Result<Self, Self::Error> {
        let termination_reason = data_model::ContainerTerminationReason::try_from(
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

        // Get container_type from description (moved from ContainerState)
        let container_type = description
            .map(|d| match d.container_type() {
                ContainerTypePb::Unknown => ContainerType::Function, /* Default for backwards */
                // compat
                ContainerTypePb::Function => ContainerType::Function,
                ContainerTypePb::Sandbox => ContainerType::Sandbox,
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
            ContainerStatus::Unknown => data_model::ContainerState::Unknown,
            ContainerStatus::Pending => data_model::ContainerState::Pending,
            ContainerStatus::Running => data_model::ContainerState::Running,
            ContainerStatus::Terminated => data_model::ContainerState::Terminated {
                reason: termination_reason,
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
            .id(data_model::ContainerId::new(id.clone()))
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

pub(crate) fn to_internal_function_arg(
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

pub(crate) fn to_internal_function_call(
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

pub(crate) fn to_internal_reduce_op(
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

pub(crate) fn to_internal_compute_op(
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

/// Convert a proto `AllocationFailureReason` to the internal
/// `FunctionRunFailureReason`.
pub(crate) fn proto_failure_reason_to_internal(
    reason: executor_api_pb::AllocationFailureReason,
) -> data_model::FunctionRunFailureReason {
    match reason {
        executor_api_pb::AllocationFailureReason::Unknown => {
            data_model::FunctionRunFailureReason::Unknown
        }
        executor_api_pb::AllocationFailureReason::InternalError => {
            data_model::FunctionRunFailureReason::InternalError
        }
        executor_api_pb::AllocationFailureReason::FunctionError => {
            data_model::FunctionRunFailureReason::FunctionError
        }
        executor_api_pb::AllocationFailureReason::FunctionTimeout => {
            data_model::FunctionRunFailureReason::FunctionTimeout
        }
        executor_api_pb::AllocationFailureReason::RequestError => {
            data_model::FunctionRunFailureReason::RequestError
        }
        executor_api_pb::AllocationFailureReason::AllocationCancelled => {
            data_model::FunctionRunFailureReason::FunctionRunCancelled
        }
        executor_api_pb::AllocationFailureReason::ContainerTerminated => {
            data_model::FunctionRunFailureReason::FunctionExecutorTerminated
        }
        executor_api_pb::AllocationFailureReason::Oom => {
            data_model::FunctionRunFailureReason::OutOfMemory
        }
        executor_api_pb::AllocationFailureReason::ConstraintUnsatisfiable => {
            data_model::FunctionRunFailureReason::ConstraintUnsatisfiable
        }
        executor_api_pb::AllocationFailureReason::ExecutorRemoved => {
            data_model::FunctionRunFailureReason::ExecutorRemoved
        }
        executor_api_pb::AllocationFailureReason::StartupFailedInternalError => {
            data_model::FunctionRunFailureReason::ContainerStartupInternalError
        }
        executor_api_pb::AllocationFailureReason::StartupFailedFunctionError => {
            data_model::FunctionRunFailureReason::ContainerStartupFunctionError
        }
        executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout => {
            data_model::FunctionRunFailureReason::ContainerStartupFunctionTimeout
        }
        executor_api_pb::AllocationFailureReason::StartupFailedBadImage => {
            data_model::FunctionRunFailureReason::ContainerStartupBadImage
        }
    }
}

/// Derive a `ContainerTerminationReason` from an `AllocationFailureReason`.
pub(crate) fn proto_failure_reason_to_termination_reason(
    reason: executor_api_pb::AllocationFailureReason,
) -> data_model::ContainerTerminationReason {
    match reason {
        executor_api_pb::AllocationFailureReason::StartupFailedInternalError => {
            data_model::ContainerTerminationReason::StartupFailedInternalError
        }
        executor_api_pb::AllocationFailureReason::StartupFailedFunctionError => {
            data_model::ContainerTerminationReason::StartupFailedFunctionError
        }
        executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout => {
            data_model::ContainerTerminationReason::StartupFailedFunctionTimeout
        }
        executor_api_pb::AllocationFailureReason::StartupFailedBadImage => {
            data_model::ContainerTerminationReason::StartupFailedBadImage
        }
        executor_api_pb::AllocationFailureReason::Oom => {
            data_model::ContainerTerminationReason::Oom
        }
        executor_api_pb::AllocationFailureReason::FunctionTimeout => {
            data_model::ContainerTerminationReason::FunctionTimeout
        }
        executor_api_pb::AllocationFailureReason::FunctionError |
        executor_api_pb::AllocationFailureReason::ContainerTerminated => {
            data_model::ContainerTerminationReason::Unhealthy
        }
        _ => data_model::ContainerTerminationReason::Unknown,
    }
}

/// Convert a proto `ContainerTerminationReason` to the internal
/// `ContainerTerminationReason`.
pub(crate) fn proto_container_termination_to_internal(
    reason: executor_api_pb::ContainerTerminationReason,
) -> data_model::ContainerTerminationReason {
    match reason {
        executor_api_pb::ContainerTerminationReason::Unknown => {
            data_model::ContainerTerminationReason::Unknown
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedInternalError => {
            data_model::ContainerTerminationReason::StartupFailedInternalError
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedFunctionError => {
            data_model::ContainerTerminationReason::StartupFailedFunctionError
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedFunctionTimeout => {
            data_model::ContainerTerminationReason::StartupFailedFunctionTimeout
        }
        executor_api_pb::ContainerTerminationReason::Unhealthy => {
            data_model::ContainerTerminationReason::Unhealthy
        }
        executor_api_pb::ContainerTerminationReason::InternalError => {
            data_model::ContainerTerminationReason::InternalError
        }
        executor_api_pb::ContainerTerminationReason::FunctionTimeout => {
            data_model::ContainerTerminationReason::FunctionTimeout
        }
        executor_api_pb::ContainerTerminationReason::FunctionCancelled => {
            data_model::ContainerTerminationReason::FunctionCancelled
        }
        executor_api_pb::ContainerTerminationReason::Oom => {
            data_model::ContainerTerminationReason::Oom
        }
        executor_api_pb::ContainerTerminationReason::ProcessCrash => {
            data_model::ContainerTerminationReason::ProcessCrash
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedBadImage => {
            data_model::ContainerTerminationReason::StartupFailedBadImage
        }
    }
}

pub(crate) fn prepare_data_payload(
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
