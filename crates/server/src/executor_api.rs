use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    vec,
};

use anyhow::Result;
use executor_api_pb::{
    AllowedFunction,
    DataPayload as DataPayloadPb,
    DataPayloadEncoding,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorResources,
    FunctionExecutorStatus,
    FunctionExecutorType as FunctionExecutorTypePb,
    HostResources,
    executor_api_server::ExecutorApi,
};
pub use proto_api::executor_api_pb;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::{error, info, trace, warn};

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
        GPUResources,
    },
    executor_api::executor_api_pb::{FunctionExecutorState, FunctionExecutorTerminationReason},
    executors::ExecutorManager,
    pb_helpers::{blob_store_path_to_url, blob_store_url_to_path, fn_call_outcome_to_pb},
    state_store::{
        ExecutorEvent,
        IndexifyState,
        executor_watches::ExecutorWatch,
        in_memory_state::FunctionCallOutcome,
        requests::{
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

/// Shared helper for converting proto watcher-like messages to `ExecutorWatch`.
/// All three proto types (`FunctionCallWatch`, `AddWatcherRequest`,
/// `RemoveWatcherRequest`) carry the same four required fields.
fn try_into_executor_watch(
    namespace: Option<String>,
    application: Option<String>,
    request_id: Option<String>,
    function_call_id: Option<String>,
    type_name: &str,
) -> Result<ExecutorWatch> {
    Ok(ExecutorWatch {
        namespace: namespace.ok_or_else(|| anyhow::anyhow!("Missing namespace in {type_name}"))?,
        application: application
            .ok_or_else(|| anyhow::anyhow!("Missing application in {type_name}"))?,
        request_id: request_id
            .ok_or_else(|| anyhow::anyhow!("Missing request_id in {type_name}"))?,
        function_call_id: function_call_id
            .ok_or_else(|| anyhow::anyhow!("Missing function_call_id in {type_name}"))?,
    })
}

impl TryFrom<&executor_api_pb::FunctionCallWatch> for ExecutorWatch {
    type Error = anyhow::Error;

    fn try_from(value: &executor_api_pb::FunctionCallWatch) -> Result<Self> {
        try_into_executor_watch(
            value.namespace.clone(),
            value.application.clone(),
            value.request_id.clone(),
            value.function_call_id.clone(),
            "FunctionCallWatch",
        )
    }
}

impl TryFrom<&executor_api_pb::AddWatcherRequest> for ExecutorWatch {
    type Error = anyhow::Error;

    fn try_from(value: &executor_api_pb::AddWatcherRequest) -> Result<Self> {
        try_into_executor_watch(
            value.namespace.clone(),
            value.application.clone(),
            value.request_id.clone(),
            value.function_call_id.clone(),
            "AddWatcherRequest",
        )
    }
}

impl TryFrom<&executor_api_pb::RemoveWatcherRequest> for ExecutorWatch {
    type Error = anyhow::Error;

    fn try_from(value: &executor_api_pb::RemoveWatcherRequest) -> Result<Self> {
        try_into_executor_watch(
            value.namespace.clone(),
            value.application.clone(),
            value.request_id.clone(),
            value.function_call_id.clone(),
            "RemoveWatcherRequest",
        )
    }
}

/// Pure, stateful diff engine that compares the current `DesiredExecutorState`
/// against what it previously saw and produces typed `Command` messages.
///
/// On the first call, the tracking sets are empty so everything is "new" —
/// producing AddContainer + RunAllocation + DeliverResult for full state
/// (equivalent to initial sync).
///
/// `command_seq = 0` means the command is informational / unsolicited.
pub struct CommandEmitter {
    next_seq: u64,
    /// Container IDs sent via AddContainer.
    pub(crate) known_containers: HashSet<String>,
    /// Allocation IDs sent via RunAllocation.
    pub(crate) known_allocations: HashSet<String>,
    /// Function call result IDs sent via DeliverResult (keyed by
    /// function_call_id).
    pub(crate) known_call_results: HashSet<String>,
}

impl CommandEmitter {
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            known_containers: HashSet::new(),
            known_allocations: HashSet::new(),
            known_call_results: HashSet::new(),
        }
    }

    /// Track an allocation ID so FullSync won't re-emit it.
    pub fn track_allocation(&mut self, id: String) {
        self.known_allocations.insert(id);
    }

    /// Track a container ID so FullSync won't re-emit it.
    pub fn track_container(&mut self, id: String) {
        self.known_containers.insert(id);
    }

    /// Untrack a container ID (removed).
    pub fn untrack_container(&mut self, id: &str) {
        self.known_containers.remove(id);
    }

    /// Track a call result ID so FullSync won't re-emit it.
    pub fn track_call_result(&mut self, id: String) {
        self.known_call_results.insert(id);
    }

    pub(crate) fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Diff the current desired state against what was previously seen and
    /// produce a batch of `Command` messages for the delta.
    pub fn emit_commands(
        &mut self,
        desired_state: &executor_api_pb::DesiredExecutorState,
    ) -> Vec<executor_api_pb::Command> {
        let mut commands = Vec::new();

        // --- Containers ---
        let current_containers: HashSet<String> = desired_state
            .function_executors
            .iter()
            .filter_map(|fe| fe.id.clone())
            .collect();

        // New containers → AddContainer
        for fe in &desired_state.function_executors {
            if let Some(id) = &fe.id &&
                !self.known_containers.contains(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::AddContainer(
                        executor_api_pb::AddContainer {
                            container: Some(fe.clone()),
                        },
                    )),
                });
            }
        }

        // Removed containers → RemoveContainer
        let removed_containers: Vec<String> = self
            .known_containers
            .iter()
            .filter(|id| !current_containers.contains(*id))
            .cloned()
            .collect();
        for id in removed_containers {
            let seq = self.next_seq();
            commands.push(executor_api_pb::Command {
                seq,
                command: Some(executor_api_pb::command::Command::RemoveContainer(
                    executor_api_pb::RemoveContainer {
                        container_id: id,
                        reason: None, // Reason comes from server state machine, not snapshot diff
                    },
                )),
            });
        }

        self.known_containers = current_containers;

        // --- Allocations ---
        let current_allocations: HashSet<String> = desired_state
            .allocations
            .iter()
            .filter_map(|a| a.allocation_id.clone())
            .collect();

        // New allocations → RunAllocation
        for allocation in &desired_state.allocations {
            if let Some(id) = &allocation.allocation_id &&
                !self.known_allocations.contains(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::RunAllocation(
                        executor_api_pb::RunAllocation {
                            allocation: Some(allocation.clone()),
                        },
                    )),
                });
            }
        }

        // Allocations that disappear are completed, not killed. We just stop tracking.
        self.known_allocations = current_allocations;

        // --- Function call results → DeliverResult ---
        let current_results: HashSet<String> = desired_state
            .function_call_results
            .iter()
            .filter_map(|r| r.function_call_id.clone())
            .collect();

        for result in &desired_state.function_call_results {
            if let Some(id) = &result.function_call_id &&
                !self.known_call_results.contains(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::DeliverResult(
                        executor_api_pb::DeliverResult {
                            result: Some(result.clone()),
                        },
                    )),
                });
            }
        }

        self.known_call_results = current_results;

        commands
    }
}

/// Register an executor's full state — the shared business logic behind
/// both the v2 heartbeat RPC (with `full_state`) and the test
/// infrastructure.
///
/// 1. Updates runtime data via `executor_manager.register_executor()`
/// 2. Writes `UpsertExecutor` to the state machine with the provided executor
///    metadata and watches.
///
/// Callers are responsible for calling `heartbeat_v2()` for liveness
/// before or after this function.
pub async fn sync_executor_full_state(
    executor_manager: &ExecutorManager,
    indexify_state: Arc<IndexifyState>,
    executor: ExecutorMetadata,
    watch_function_calls: HashSet<ExecutorWatch>,
) -> Result<()> {
    // Register runtime data (state hash + clock)
    executor_manager.register_executor(executor.clone()).await?;

    // Build and write UpsertExecutor to the state machine
    let upsert_request = UpsertExecutorRequest::build(
        executor,
        vec![], // allocation_outputs come through a separate path
        true,   // v2 full state sync always updates executor state
        watch_function_calls,
        indexify_state.clone(),
    )?;

    let sm_req = StateMachineUpdateRequest {
        payload: RequestPayload::UpsertExecutor(upsert_request),
    };
    indexify_state.write(sm_req).await?;

    Ok(())
}

/// Convert a proto `AllocationFailureReason` to the internal
/// `FunctionRunFailureReason`. This is the reverse of
/// `pb_helpers::fn_call_outcome_to_pb`'s failure_reason mapping.
fn proto_failure_reason_to_internal(
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
        executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated => {
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
    }
}

/// Convert a proto `ContainerTerminationReason` to the internal
/// `FunctionExecutorTerminationReason`.
fn proto_container_termination_to_internal(
    reason: executor_api_pb::ContainerTerminationReason,
) -> data_model::FunctionExecutorTerminationReason {
    match reason {
        executor_api_pb::ContainerTerminationReason::Unknown => {
            data_model::FunctionExecutorTerminationReason::Unknown
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedInternalError => {
            data_model::FunctionExecutorTerminationReason::StartupFailedInternalError
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedFunctionError => {
            data_model::FunctionExecutorTerminationReason::StartupFailedFunctionError
        }
        executor_api_pb::ContainerTerminationReason::StartupFailedFunctionTimeout => {
            data_model::FunctionExecutorTerminationReason::StartupFailedFunctionTimeout
        }
        executor_api_pb::ContainerTerminationReason::Unhealthy => {
            data_model::FunctionExecutorTerminationReason::Unhealthy
        }
        executor_api_pb::ContainerTerminationReason::InternalError => {
            data_model::FunctionExecutorTerminationReason::InternalError
        }
        executor_api_pb::ContainerTerminationReason::FunctionTimeout => {
            data_model::FunctionExecutorTerminationReason::FunctionTimeout
        }
        executor_api_pb::ContainerTerminationReason::FunctionCancelled => {
            data_model::FunctionExecutorTerminationReason::FunctionCancelled
        }
        executor_api_pb::ContainerTerminationReason::Oom => {
            data_model::FunctionExecutorTerminationReason::Oom
        }
        executor_api_pb::ContainerTerminationReason::ProcessCrash => {
            data_model::FunctionExecutorTerminationReason::ProcessCrash
        }
    }
}

/// Process command responses from a dataplane executor.
///
/// Converts proto `CommandResponse` messages into a single
/// `DataplaneResultsIngestedEvent` and writes it to the state machine via
/// `RequestPayload::DataplaneResults`. This is the shared logic used by both
/// the `report_command_responses` RPC handler and tests.
pub async fn process_command_responses(
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
    executor_id: &ExecutorId,
    responses: Vec<executor_api_pb::CommandResponse>,
) -> Result<()> {
    use data_model::{
        AllocationOutputIngestedEvent,
        ContainerStateUpdateInfo,
        DataplaneResultsIngestedEvent,
        FunctionRunOutcome,
        GraphUpdates,
    };

    let mut allocation_events = Vec::new();
    let mut container_state_updates = Vec::new();
    let mut container_started_ids = Vec::new();

    for resp in responses {
        let Some(response) = resp.response else {
            warn!("CommandResponse with no response oneof, skipping");
            continue;
        };

        match response {
            executor_api_pb::command_response::Response::AllocationCompleted(completed) => {
                let function = completed
                    .function
                    .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function"))?;
                let namespace = function
                    .namespace
                    .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing namespace"))?;
                let application = function.application_name.ok_or_else(|| {
                    anyhow::anyhow!("AllocationCompleted missing application_name")
                })?;
                let fn_name = function
                    .function_name
                    .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function_name"))?;
                let request_id = completed
                    .request_id
                    .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing request_id"))?;
                let function_call_id = completed.function_call_id.ok_or_else(|| {
                    anyhow::anyhow!("AllocationCompleted missing function_call_id")
                })?;
                let allocation_id = completed.allocation_id;

                // Look up the allocation from the state store
                let allocation_key = data_model::Allocation::key_from(
                    &namespace,
                    &application,
                    &request_id,
                    &allocation_id,
                );
                let allocation = indexify_state
                    .reader()
                    .get_allocation(&allocation_key)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "AllocationCompleted: allocation not found: {}",
                            allocation_key
                        )
                    })?;

                // Convert return_value oneof
                let (data_payload, graph_updates) = match completed.return_value {
                    Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
                        let blob_store_url_scheme = blob_storage_registry
                            .get_blob_store(&namespace)
                            .get_url_scheme();
                        let blob_store_url =
                            blob_storage_registry.get_blob_store(&namespace).get_url();
                        let payload =
                            prepare_data_payload(dp, &blob_store_url_scheme, &blob_store_url)?;
                        (Some(payload), None)
                    }
                    Some(executor_api_pb::allocation_completed::ReturnValue::Updates(updates)) => {
                        let root_function_call_id = updates
                            .root_function_call_id
                            .map(FunctionCallId::from)
                            .unwrap_or_else(|| FunctionCallId::from(nanoid::nanoid!()));
                        let mut compute_ops = Vec::new();
                        for update in updates.updates {
                            compute_ops.push(to_internal_compute_op(
                                update,
                                blob_storage_registry,
                                Some(function_call_id.clone()),
                            )?);
                        }
                        (
                            None,
                            Some(GraphUpdates {
                                graph_updates: compute_ops,
                                output_function_call_id: root_function_call_id,
                            }),
                        )
                    }
                    None => (None, None),
                };

                allocation_events.push(AllocationOutputIngestedEvent {
                    namespace,
                    application,
                    function: fn_name,
                    request_id,
                    function_call_id: FunctionCallId::from(function_call_id),
                    data_payload,
                    graph_updates,
                    request_exception: None,
                    allocation_id: allocation.id,
                    allocation_target: allocation.target,
                    allocation_outcome: FunctionRunOutcome::Success,
                    execution_duration_ms: completed.execution_duration_ms,
                });
            }
            executor_api_pb::command_response::Response::AllocationFailed(failed) => {
                let failure_reason = proto_failure_reason_to_internal(failed.reason());
                let function = failed
                    .function
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function"))?;
                let namespace = function
                    .namespace
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing namespace"))?;
                let application = function
                    .application_name
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing application_name"))?;
                let fn_name = function
                    .function_name
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_name"))?;
                let request_id = failed
                    .request_id
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing request_id"))?;
                let function_call_id = failed
                    .function_call_id
                    .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_call_id"))?;
                let allocation_id = failed.allocation_id;

                // Look up the allocation from the state store
                let allocation_key = data_model::Allocation::key_from(
                    &namespace,
                    &application,
                    &request_id,
                    &allocation_id,
                );
                let allocation = indexify_state
                    .reader()
                    .get_allocation(&allocation_key)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "AllocationFailed: allocation not found: {}",
                            allocation_key
                        )
                    })?;

                // Convert request_error if present
                let request_exception = if let Some(dp) = failed.request_error {
                    let blob_store_url_scheme = blob_storage_registry
                        .get_blob_store(&namespace)
                        .get_url_scheme();
                    let blob_store_url = blob_storage_registry.get_blob_store(&namespace).get_url();
                    Some(prepare_data_payload(
                        dp,
                        &blob_store_url_scheme,
                        &blob_store_url,
                    )?)
                } else {
                    None
                };

                allocation_events.push(AllocationOutputIngestedEvent {
                    namespace,
                    application,
                    function: fn_name,
                    request_id,
                    function_call_id: FunctionCallId::from(function_call_id),
                    data_payload: None,
                    graph_updates: None,
                    request_exception,
                    allocation_id: allocation.id,
                    allocation_target: allocation.target,
                    allocation_outcome: FunctionRunOutcome::Failure(failure_reason),
                    execution_duration_ms: failed.execution_duration_ms,
                });
            }
            executor_api_pb::command_response::Response::ContainerTerminated(terminated) => {
                let reason = proto_container_termination_to_internal(terminated.reason());
                container_state_updates.push(ContainerStateUpdateInfo {
                    container_id: data_model::ContainerId::new(terminated.container_id),
                    termination_reason: Some(reason),
                });
            }
            executor_api_pb::command_response::Response::ContainerStarted(started) => {
                info!(
                    executor_id = executor_id.get(),
                    container_id = started.container_id,
                    "ContainerStarted — will promote sandbox if pending"
                );
                container_started_ids.push(data_model::ContainerId::new(started.container_id));
            }
        }
    }

    if allocation_events.is_empty() &&
        container_state_updates.is_empty() &&
        container_started_ids.is_empty()
    {
        return Ok(());
    }

    let event = DataplaneResultsIngestedEvent {
        executor_id: executor_id.clone(),
        allocation_events,
        container_state_updates,
        container_started_ids,
    };

    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DataplaneResults(
                crate::state_store::requests::DataplaneResultsRequest { event },
            ),
        })
        .await?;

    Ok(())
}

pub struct ExecutorAPIService {
    indexify_state: Arc<IndexifyState>,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
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
        }
    }

    async fn handle_v2_full_state(
        &self,
        executor_id: &ExecutorId,
        full_state: executor_api_pb::DataplaneStateFullSync,
    ) -> Result<(), Status> {
        // --- Proto → internal conversion ---

        let mut executor_metadata = ExecutorMetadataBuilder::default();
        executor_metadata.id(executor_id.clone());
        executor_metadata.addr(full_state.hostname.clone().unwrap_or_default());
        executor_metadata.executor_version(full_state.version.unwrap_or_default());
        if let Some(catalog_name) = full_state.catalog_entry_name {
            executor_metadata.catalog_name(Some(catalog_name));
        }
        if let Some(proxy_address) = full_state.proxy_address {
            executor_metadata.proxy_address(Some(proxy_address));
        }

        let host_resources = full_state
            .total_function_executor_resources
            .map(data_model::HostResources::try_from)
            .transpose()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?
            .unwrap_or_default();
        executor_metadata.host_resources(host_resources);

        let allowed_functions: Vec<FunctionAllowlist> = full_state
            .allowed_functions
            .into_iter()
            .filter_map(|f| FunctionAllowlist::try_from(f).ok())
            .collect();
        if allowed_functions.is_empty() {
            executor_metadata.function_allowlist(None);
        } else {
            executor_metadata.function_allowlist(Some(allowed_functions));
        }
        executor_metadata.labels(full_state.labels);
        executor_metadata.state(data_model::ExecutorState::Running);
        executor_metadata.state_hash(String::new());

        let mut containers = HashMap::new();
        for fe_state in full_state.function_executor_states {
            match data_model::Container::try_from(fe_state) {
                Ok(container) => {
                    containers.insert(container.id.clone(), container);
                }
                Err(e) => {
                    warn!(
                        executor_id = executor_id.get(),
                        error = %e,
                        "skipping container in full state sync"
                    );
                }
            }
        }
        executor_metadata.containers(containers);

        let executor = executor_metadata
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut watch_function_calls = HashSet::new();
        for function_call_watch in &full_state.function_call_watches {
            match ExecutorWatch::try_from(function_call_watch) {
                Ok(watch) => {
                    watch_function_calls.insert(watch);
                }
                Err(e) => {
                    warn!(
                        executor_id = executor_id.get(),
                        error = %e,
                        "skipping invalid watch in full state sync"
                    );
                }
            }
        }

        // --- Shared registration logic ---
        sync_executor_full_state(
            &self.executor_manager,
            self.indexify_state.clone(),
            executor,
            watch_function_calls,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }
}

/// Build a RunAllocation command from an internal `Allocation` model.
fn build_run_allocation_command(
    emitter: &mut CommandEmitter,
    allocation: &data_model::Allocation,
    blob_storage_registry: &BlobStorageRegistry,
) -> executor_api_pb::Command {
    let blob_store_url_scheme = blob_storage_registry
        .get_blob_store(&allocation.namespace)
        .get_url_scheme();
    let blob_store_url = blob_storage_registry
        .get_blob_store(&allocation.namespace)
        .get_url();

    let mut args = vec![];
    for input_arg in &allocation.input_args {
        args.push(executor_api_pb::DataPayload {
            id: Some(input_arg.data_payload.id.clone()),
            uri: Some(blob_store_path_to_url(
                &input_arg.data_payload.path,
                &blob_store_url_scheme,
                &blob_store_url,
            )),
            size: Some(input_arg.data_payload.size),
            sha256_hash: Some(input_arg.data_payload.sha256_hash.clone()),
            encoding: Some(
                crate::pb_helpers::string_to_data_payload_encoding(
                    &input_arg.data_payload.encoding,
                )
                .into(),
            ),
            encoding_version: Some(0),
            offset: Some(input_arg.data_payload.offset),
            metadata_size: Some(input_arg.data_payload.metadata_size),
            source_function_call_id: input_arg.function_call_id.as_ref().map(|id| id.to_string()),
            content_type: Some(input_arg.data_payload.encoding.clone()),
        });
    }

    let request_data_payload_uri_prefix = format!(
        "{}/{}",
        blob_store_url,
        data_model::DataPayload::request_key_prefix(
            &allocation.namespace,
            &allocation.application,
            &allocation.request_id,
        ),
    );

    let allocation_pb = executor_api_pb::Allocation {
        function: Some(executor_api_pb::FunctionRef {
            namespace: Some(allocation.namespace.clone()),
            application_name: Some(allocation.application.clone()),
            function_name: Some(allocation.function.clone()),
            application_version: None,
        }),
        function_executor_id: Some(allocation.target.function_executor_id.get().to_string()),
        allocation_id: Some(allocation.id.to_string()),
        function_call_id: Some(allocation.function_call_id.to_string()),
        request_id: Some(allocation.request_id.to_string()),
        args,
        request_data_payload_uri_prefix: Some(request_data_payload_uri_prefix.clone()),
        request_error_payload_uri_prefix: Some(request_data_payload_uri_prefix),
        function_call_metadata: Some(allocation.call_metadata.clone().into()),
        replay_mode: None,
        last_event_clock: None,
    };

    let seq = emitter.next_seq();
    executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::RunAllocation(
            executor_api_pb::RunAllocation {
                allocation: Some(allocation_pb),
            },
        )),
    }
}

/// Build an AddContainer command by looking up container data from state.
async fn build_add_container_command(
    emitter: &mut CommandEmitter,
    container_id: &ContainerId,
    blob_storage_registry: &BlobStorageRegistry,
    indexify_state: &IndexifyState,
) -> Option<executor_api_pb::Command> {
    let container_scheduler = indexify_state.container_scheduler.read().await;
    let fc = container_scheduler.function_containers.get(container_id)?;

    // Skip terminated containers
    if matches!(
        fc.desired_state,
        data_model::ContainerState::Terminated { .. }
    ) {
        return None;
    }

    let fe = &fc.function_container;
    let indexes = indexify_state.in_memory_state.read().await;

    let cg_version = indexes
        .application_versions
        .get(&data_model::ApplicationVersion::key_from(
            &fe.namespace,
            &fe.application_name,
            &fe.version,
        ))
        .cloned();

    let cg_node = cg_version
        .as_ref()
        .and_then(|v| v.functions.get(&fe.function_name).cloned());

    let code_payload_pb = cg_version.and_then(|v| v.code).map(|code| {
        let blob_store_url_scheme = blob_storage_registry
            .get_blob_store(&fe.namespace)
            .get_url_scheme();
        let blob_store_url = blob_storage_registry
            .get_blob_store(&fe.namespace)
            .get_url();
        executor_api_pb::DataPayload {
            id: Some(code.id.clone()),
            uri: Some(blob_store_path_to_url(
                &code.path,
                &blob_store_url_scheme,
                &blob_store_url,
            )),
            size: Some(code.size),
            sha256_hash: Some(code.sha256_hash.clone()),
            encoding: Some(executor_api_pb::DataPayloadEncoding::BinaryZip.into()),
            encoding_version: Some(0),
            offset: Some(0),
            metadata_size: Some(0),
            source_function_call_id: None,
            content_type: Some("application/zip".to_string()),
        }
    });

    let fe_type_pb = match fe.container_type {
        data_model::ContainerType::Function => executor_api_pb::FunctionExecutorType::Function,
        data_model::ContainerType::Sandbox => executor_api_pb::FunctionExecutorType::Sandbox,
    };

    let network_policy_pb = fe
        .network_policy
        .as_ref()
        .map(|np| executor_api_pb::NetworkPolicy {
            allow_internet_access: Some(np.allow_internet_access),
            allow_out: np.allow_out.clone(),
            deny_out: np.deny_out.clone(),
        });

    let sandbox_metadata = if fe.container_type == data_model::ContainerType::Sandbox {
        Some(executor_api_pb::SandboxMetadata {
            image: fe.image.clone(),
            timeout_secs: if fe.timeout_secs > 0 {
                Some(fe.timeout_secs)
            } else {
                None
            },
            entrypoint: fe.entrypoint.clone(),
            network_policy: network_policy_pb,
            sandbox_id: fe.sandbox_id.as_ref().map(|s| s.get().to_string()),
        })
    } else {
        None
    };

    let resources_pb: Option<executor_api_pb::FunctionExecutorResources> =
        fe.resources.clone().try_into().ok();

    let initialization_timeout_ms = cg_node
        .as_ref()
        .map(|n| n.initialization_timeout.0)
        .unwrap_or_else(|| {
            fe.timeout_secs
                .saturating_mul(1000)
                .try_into()
                .unwrap_or(u32::MAX)
        });

    let allocation_timeout_ms = cg_node.map(|n| n.timeout.0).unwrap_or_else(|| {
        fe.timeout_secs
            .saturating_mul(1000)
            .try_into()
            .unwrap_or(u32::MAX)
    });

    let fe_description_pb = executor_api_pb::FunctionExecutorDescription {
        id: Some(fe.id.get().to_string()),
        function: Some(executor_api_pb::FunctionRef {
            namespace: Some(fe.namespace.clone()),
            application_name: Some(fe.application_name.clone()),
            function_name: Some(fe.function_name.clone()),
            application_version: Some(fe.version.to_string()),
        }),
        secret_names: cg_node_secret_names(&indexes, fe),
        initialization_timeout_ms: Some(initialization_timeout_ms),
        application: code_payload_pb,
        allocation_timeout_ms: Some(allocation_timeout_ms),
        resources: resources_pb,
        max_concurrency: Some(fe.max_concurrency),
        sandbox_metadata,
        container_type: Some(fe_type_pb.into()),
        pool_id: fe.pool_id.as_ref().map(|p| p.get().to_string()),
    };

    drop(indexes);
    drop(container_scheduler);

    let seq = emitter.next_seq();
    Some(executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::AddContainer(
            executor_api_pb::AddContainer {
                container: Some(fe_description_pb),
            },
        )),
    })
}

/// Helper to get secret_names from in-memory state for a container.
fn cg_node_secret_names(
    indexes: &crate::state_store::in_memory_state::InMemoryState,
    fe: &data_model::Container,
) -> Vec<String> {
    indexes
        .application_versions
        .get(&data_model::ApplicationVersion::key_from(
            &fe.namespace,
            &fe.application_name,
            &fe.version,
        ))
        .and_then(|v| v.functions.get(&fe.function_name))
        .and_then(|n| n.secret_names.clone())
        .unwrap_or_default()
}

/// Build a DeliverResult command for a completed watch.
async fn build_deliver_result_command(
    emitter: &mut CommandEmitter,
    namespace: &str,
    application: &str,
    request_id: &str,
    function_call_id: &str,
    blob_storage_registry: &BlobStorageRegistry,
    indexify_state: &IndexifyState,
) -> Option<executor_api_pb::Command> {
    let indexes = indexify_state.in_memory_state.read().await;

    let watch_key = ExecutorWatch {
        namespace: namespace.to_string(),
        application: application.to_string(),
        request_id: request_id.to_string(),
        function_call_id: function_call_id.to_string(),
    };

    let function_run = if let Some(fr) = indexes.function_runs.get(&(&watch_key).into()) {
        fr.clone()
    } else {
        drop(indexes);
        // Fall back to RocksDB
        let reader = indexify_state.reader();
        let fr = reader
            .get_function_run(namespace, application, request_id, function_call_id)
            .await?;
        Box::new(fr)
    };

    // Skip if not completed
    let outcome = function_run.outcome.as_ref()?;

    let failure_reason = match outcome {
        data_model::FunctionRunOutcome::Failure(reason) => Some(reason.clone()),
        _ => None,
    };

    let call_outcome = FunctionCallOutcome {
        namespace: function_run.namespace.clone(),
        request_id: function_run.request_id.clone(),
        function_call_id: function_run.id.clone(),
        outcome: outcome.clone(),
        failure_reason,
        return_value: function_run.output.clone(),
        request_error: function_run.request_error.clone(),
    };

    let blob_store_url_scheme = blob_storage_registry
        .get_blob_store(namespace)
        .get_url_scheme();
    let blob_store_url = blob_storage_registry.get_blob_store(namespace).get_url();

    let result_pb = fn_call_outcome_to_pb(&call_outcome, &blob_store_url_scheme, &blob_store_url);

    let seq = emitter.next_seq();
    Some(executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::DeliverResult(
            executor_api_pb::DeliverResult {
                result: Some(result_pb),
            },
        )),
    })
}

/// Perform a full sync: fetch complete executor state, diff via emitter,
/// and send all resulting commands.
async fn do_full_sync(
    executor_id: &ExecutorId,
    executor_manager: &ExecutorManager,
    emitter: &mut CommandEmitter,
    grpc_tx: &tokio::sync::mpsc::Sender<Result<executor_api_pb::Command, Status>>,
) -> bool {
    let Some(snapshot) = executor_manager.get_executor_state(executor_id).await else {
        trace!(
            executor_id = executor_id.get(),
            "command_stream: executor state not available for full sync"
        );
        return true; // continue loop
    };

    let commands = emitter.emit_commands(&snapshot.desired_state);
    if !commands.is_empty() {
        info!(
            executor_id = executor_id.get(),
            num_commands = commands.len(),
            "command_stream: full sync emitting commands"
        );
    }
    for cmd in commands {
        if grpc_tx.send(Ok(cmd)).await.is_err() {
            info!(
                executor_id = executor_id.get(),
                "command_stream: send failed, client disconnected"
            );
            return false; // exit loop
        }
    }
    true // continue loop
}

/// Background task that drives a single command_stream for one executor.
/// Consumes typed `ExecutorEvent`s and converts them to proto `Command`
/// messages. Falls back to full-state diff for initial sync and `FullSync`
/// events.
async fn command_stream_loop(
    executor_id: ExecutorId,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
    indexify_state: Arc<IndexifyState>,
    mut event_rx: tokio::sync::mpsc::UnboundedReceiver<ExecutorEvent>,
    grpc_tx: tokio::sync::mpsc::Sender<Result<executor_api_pb::Command, Status>>,
) {
    let mut emitter = CommandEmitter::new();

    // Initial full sync — populates CommandEmitter tracking sets.
    if !do_full_sync(&executor_id, &executor_manager, &mut emitter, &grpc_tx).await {
        indexify_state.deregister_event_channel(&executor_id).await;
        return;
    }

    loop {
        tokio::select! {
            _ = grpc_tx.closed() => {
                info!(
                    executor_id = executor_id.get(),
                    "command_stream: client disconnected"
                );
                break;
            }
            event = event_rx.recv() => {
                let Some(event) = event else {
                    info!(
                        executor_id = executor_id.get(),
                        "command_stream: event channel closed"
                    );
                    break;
                };

                match event {
                    ExecutorEvent::AllocationCreated(allocation) => {
                        let alloc_id = allocation.id.to_string();
                        if emitter.known_allocations.contains(&alloc_id) {
                            continue; // Already tracked — dedup
                        }
                        let cmd = build_run_allocation_command(
                            &mut emitter,
                            &allocation,
                            &blob_storage_registry,
                        );
                        emitter.track_allocation(alloc_id);
                        info!(
                            executor_id = executor_id.get(),
                            allocation_id = %allocation.id,
                            "command_stream: emitting RunAllocation"
                        );
                        if grpc_tx.send(Ok(cmd)).await.is_err() {
                            break;
                        }
                    }
                    ExecutorEvent::ContainerAdded(container_id) => {
                        let cid = container_id.get().to_string();
                        if emitter.known_containers.contains(&cid) {
                            continue; // Already tracked
                        }
                        if let Some(cmd) = build_add_container_command(
                            &mut emitter,
                            &container_id,
                            &blob_storage_registry,
                            &indexify_state,
                        ).await {
                            emitter.track_container(cid);
                            info!(
                                executor_id = executor_id.get(),
                                container_id = container_id.get(),
                                "command_stream: emitting AddContainer"
                            );
                            if grpc_tx.send(Ok(cmd)).await.is_err() {
                                break;
                            }
                        }
                    }
                    ExecutorEvent::ContainerRemoved(container_id) => {
                        let cid = container_id.get().to_string();
                        emitter.untrack_container(&cid);
                        let seq = emitter.next_seq();
                        let cmd = executor_api_pb::Command {
                            seq,
                            command: Some(executor_api_pb::command::Command::RemoveContainer(
                                executor_api_pb::RemoveContainer {
                                    container_id: cid.clone(),
                                    reason: None,
                                },
                            )),
                        };
                        info!(
                            executor_id = executor_id.get(),
                            container_id = %cid,
                            "command_stream: emitting RemoveContainer"
                        );
                        if grpc_tx.send(Ok(cmd)).await.is_err() {
                            break;
                        }
                    }
                    ExecutorEvent::WatchCompleted {
                        namespace,
                        application,
                        request_id,
                        function_call_id,
                    } => {
                        if emitter.known_call_results.contains(&function_call_id) {
                            continue;
                        }
                        if let Some(cmd) = build_deliver_result_command(
                            &mut emitter,
                            &namespace,
                            &application,
                            &request_id,
                            &function_call_id,
                            &blob_storage_registry,
                            &indexify_state,
                        ).await {
                            emitter.track_call_result(function_call_id.clone());
                            info!(
                                executor_id = executor_id.get(),
                                function_call_id = %function_call_id,
                                "command_stream: emitting DeliverResult"
                            );
                            if grpc_tx.send(Ok(cmd)).await.is_err() {
                                break;
                            }
                        }
                    }
                    ExecutorEvent::FullSync => {
                        if !do_full_sync(
                            &executor_id,
                            &executor_manager,
                            &mut emitter,
                            &grpc_tx,
                        ).await {
                            break;
                        }
                    }
                }
            }
        }
    }

    indexify_state.deregister_event_channel(&executor_id).await;
}

#[tonic::async_trait]
impl ExecutorApi for ExecutorAPIService {
    #[allow(non_camel_case_types)]
    type command_streamStream =
        Pin<Box<dyn Stream<Item = Result<executor_api_pb::Command, Status>> + Send>>;

    async fn heartbeat(
        &self,
        request: Request<executor_api_pb::HeartbeatRequest>,
    ) -> Result<Response<executor_api_pb::HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let executor_id = req
            .executor_id
            .ok_or(Status::invalid_argument("executor_id required"))?
            .into();

        // Touch the executor liveness
        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Process full state if present
        let had_state = req.full_state.is_some();
        if let Some(full_state) = req.full_state {
            info!(
                executor_id = executor_id.get(),
                "processing full state sync"
            );
            self.handle_v2_full_state(&executor_id, full_state).await?;
        }

        // If no state was sent, check if the server knows this executor.
        // If unknown, ask the executor to send full state on next heartbeat.
        let send_state = if had_state {
            false
        } else {
            let runtime_data = self.executor_manager.runtime_data_read().await;
            !runtime_data.contains_key(&executor_id)
        };

        Ok(Response::new(executor_api_pb::HeartbeatResponse {
            send_state: Some(send_state),
        }))
    }

    async fn command_stream(
        &self,
        request: Request<executor_api_pb::GetCommandStreamRequest>,
    ) -> Result<Response<Self::command_streamStream>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        trace!(
            executor_id = executor_id.get(),
            "Got command_stream request",
        );

        // Verify executor is registered before setting up the stream.
        {
            let runtime_data = self.executor_manager.runtime_data_read().await;
            if !runtime_data.contains_key(&executor_id) {
                let msg = "executor not found, or not yet registered";
                warn!(executor_id = executor_id.get(), "command_stream: {}", msg);
                return Err(Status::not_found(msg));
            }
        }

        // Register event channel BEFORE initial sync so no events are missed.
        let event_rx = self
            .indexify_state
            .register_event_channel(executor_id.clone())
            .await;

        let (grpc_tx, grpc_rx) =
            tokio::sync::mpsc::channel::<Result<executor_api_pb::Command, Status>>(32);

        tokio::spawn(command_stream_loop(
            executor_id,
            self.executor_manager.clone(),
            self.blob_storage_registry.clone(),
            self.indexify_state.clone(),
            event_rx,
            grpc_tx,
        ));

        Ok(Response::new(
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(grpc_rx))
                as Self::command_streamStream,
        ))
    }

    async fn report_command_responses(
        &self,
        request: Request<executor_api_pb::ReportCommandResponsesRequest>,
    ) -> Result<Response<executor_api_pb::ReportCommandResponsesResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        // Refresh liveness — report_command_responses counts as a heartbeat
        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        info!(
            executor_id = executor_id.get(),
            num_responses = req.responses.len(),
            "report_command_responses"
        );

        process_command_responses(
            &self.indexify_state,
            &self.blob_storage_registry,
            &executor_id,
            req.responses,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(
            executor_api_pb::ReportCommandResponsesResponse {},
        ))
    }

    async fn add_allocation_events(
        &self,
        request: Request<executor_api_pb::AllocationEvents>,
    ) -> Result<Response<executor_api_pb::AllocationEventsResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(
            req.executor_id
                .ok_or(Status::invalid_argument("executor_id is required"))?,
        );

        for event in &req.events {
            let event = event
                .event
                .as_ref()
                .ok_or(Status::invalid_argument("event is required"))?;
            match event {
                executor_api_pb::allocation_event::Event::AddWatcher(add_req) => {
                    let watch: ExecutorWatch = add_req
                        .try_into()
                        .map_err(|e: anyhow::Error| Status::invalid_argument(e.to_string()))?;
                    info!(
                        executor_id = executor_id.get(),
                        function_call_id = %watch.function_call_id,
                        request_id = %watch.request_id,
                        "add_allocation_events: adding watcher"
                    );
                    let sm_req = StateMachineUpdateRequest {
                        payload: RequestPayload::AddExecutorWatch(
                            crate::state_store::requests::AddExecutorWatchRequest {
                                executor_id: executor_id.clone(),
                                watch,
                            },
                        ),
                    };
                    if let Err(e) = self.indexify_state.write(sm_req).await {
                        error!(
                            executor_id = executor_id.get(),
                            "failed to write AddExecutorWatch: {e:?}"
                        );
                        return Err(Status::internal("failed to write AddExecutorWatch"));
                    }
                }
                executor_api_pb::allocation_event::Event::RemoveWatcher(remove_req) => {
                    let watch: ExecutorWatch = remove_req
                        .try_into()
                        .map_err(|e: anyhow::Error| Status::invalid_argument(e.to_string()))?;
                    info!(
                        executor_id = executor_id.get(),
                        function_call_id = %watch.function_call_id,
                        request_id = %watch.request_id,
                        "add_allocation_events: removing watcher"
                    );
                    let sm_req = StateMachineUpdateRequest {
                        payload: RequestPayload::RemoveExecutorWatch(
                            crate::state_store::requests::RemoveExecutorWatchRequest {
                                executor_id: executor_id.clone(),
                                watch,
                            },
                        ),
                    };
                    if let Err(e) = self.indexify_state.write(sm_req).await {
                        error!(
                            executor_id = executor_id.get(),
                            "failed to write RemoveExecutorWatch: {e:?}"
                        );
                        return Err(Status::internal("failed to write RemoveExecutorWatch"));
                    }
                }
                executor_api_pb::allocation_event::Event::CallFunction(call_req) => {
                    let updates = call_req
                        .updates
                        .clone()
                        .ok_or(Status::invalid_argument("updates is required"))?;
                    let request_id = call_req
                        .request_id
                        .clone()
                        .ok_or(Status::invalid_argument("request_id is required"))?;
                    let source_function_call_id = call_req.source_function_call_id.clone().ok_or(
                        Status::invalid_argument("source_function_call_id is required"),
                    )?;
                    let namespace = call_req
                        .namespace
                        .clone()
                        .ok_or(Status::invalid_argument("namespace is required"))?;
                    let application_name = call_req
                        .application
                        .clone()
                        .ok_or(Status::invalid_argument("application is required"))?;
                    let root_function_call_id =
                        updates
                            .root_function_call_id
                            .clone()
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
                    info!(
                        executor_id = executor_id.get(),
                        namespace = %namespace,
                        application = %application_name,
                        request_id = %request_id,
                        "add_allocation_events: processing function call"
                    );
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
                        error!(
                            executor_id = executor_id.get(),
                            "failed to write CreateFunctionCall: {e:?}"
                        );
                        return Err(Status::internal("failed to write CreateFunctionCall"));
                    }
                }
            }
        }

        Ok(Response::new(executor_api_pb::AllocationEventsResponse {}))
    }

    async fn get_allocation_events(
        &self,
        request: Request<executor_api_pb::GetAllocationEventsRequest>,
    ) -> Result<Response<executor_api_pb::GetAllocationEventsResponse>, Status> {
        let req = request.into_inner();
        trace!(
            function_call_id = ?req.function_call_id,
            allocation_id = ?req.allocation_id,
            after_clock = ?req.after_clock,
            "get_allocation_events"
        );

        // Allocation events are processed as immediate side-effects
        // (add/remove watches, call_function) and not persisted to a
        // log yet.  Return an empty result until a persistent event
        // log is implemented.
        Ok(Response::new(
            executor_api_pb::GetAllocationEventsResponse {
                events: vec![],
                last_clock: None,
                has_more: Some(false),
            },
        ))
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
        };

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Function);
        assert!(
            container.sandbox_id.is_none(),
            "Function containers should not have sandbox_id"
        );
    }

    fn make_fe_description(id: &str) -> executor_api_pb::FunctionExecutorDescription {
        FunctionExecutorDescription {
            id: Some(id.to_string()),
            function: Some(FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("fn".to_string()),
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
        }
    }

    fn make_allocation(id: &str) -> executor_api_pb::Allocation {
        executor_api_pb::Allocation {
            function: Some(FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("fn".to_string()),
                application_version: None,
            }),
            allocation_id: Some(id.to_string()),
            function_call_id: Some(format!("fc-{id}")),
            request_id: Some("req-1".to_string()),
            args: vec![],
            request_data_payload_uri_prefix: None,
            request_error_payload_uri_prefix: None,
            function_executor_id: Some("c1".to_string()),
            function_call_metadata: None,
            replay_mode: None,
            last_event_clock: None,
        }
    }

    fn make_call_result(fc_id: &str) -> executor_api_pb::FunctionCallResult {
        executor_api_pb::FunctionCallResult {
            namespace: Some("ns".to_string()),
            request_id: Some("req-1".to_string()),
            function_call_id: Some(fc_id.to_string()),
            outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Success.into()),
            failure_reason: None,
            return_value: None,
            request_error: None,
        }
    }

    #[test]
    fn test_command_emitter_first_call_emits_full_state() {
        let mut emitter = super::CommandEmitter::new();

        let desired = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            function_call_results: vec![make_call_result("fc-result-1")],
            clock: Some(1),
        };

        let commands = emitter.emit_commands(&desired);

        // First call: everything is new
        assert_eq!(commands.len(), 3, "expected 3 commands: {commands:?}");

        let add_container = commands.iter().find(|c| {
            matches!(
                &c.command,
                Some(executor_api_pb::command::Command::AddContainer(_))
            )
        });
        assert!(add_container.is_some(), "expected AddContainer command");

        let run_alloc = commands.iter().find(|c| {
            matches!(
                &c.command,
                Some(executor_api_pb::command::Command::RunAllocation(_))
            )
        });
        assert!(run_alloc.is_some(), "expected RunAllocation command");

        let deliver_result = commands.iter().find(|c| {
            matches!(
                &c.command,
                Some(executor_api_pb::command::Command::DeliverResult(_))
            )
        });
        assert!(deliver_result.is_some(), "expected DeliverResult command");

        // Sequence numbers should be monotonically increasing
        let seqs: Vec<u64> = commands.iter().map(|c| c.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn test_command_emitter_no_change_emits_nothing() {
        let mut emitter = super::CommandEmitter::new();

        let desired = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            function_call_results: vec![],
            clock: Some(1),
        };

        // First call — full sync
        let commands = emitter.emit_commands(&desired);
        assert_eq!(commands.len(), 2);

        // Second call — same state → no commands
        let commands = emitter.emit_commands(&desired);
        assert!(commands.is_empty(), "expected 0 commands: {commands:?}");
    }

    #[test]
    fn test_command_emitter_container_removal() {
        let mut emitter = super::CommandEmitter::new();

        // First: one container
        let desired1 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![],
            function_call_results: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);

        // Second: container removed
        let desired2 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![],
            allocations: vec![],
            function_call_results: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        assert_eq!(commands.len(), 1, "{commands:?}");
        assert!(matches!(
            &commands[0].command,
            Some(executor_api_pb::command::Command::RemoveContainer(r))
            if r.container_id == "c1"
        ));
    }

    #[test]
    fn test_command_emitter_new_allocation_after_initial() {
        let mut emitter = super::CommandEmitter::new();

        // First: one container, one allocation
        let desired1 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            function_call_results: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);

        // Second: same container, new allocation added
        let desired2 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
            function_call_results: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        assert_eq!(commands.len(), 1, "{commands:?}");
        assert!(matches!(
            &commands[0].command,
            Some(executor_api_pb::command::Command::RunAllocation(r))
            if r.allocation.as_ref().unwrap().allocation_id.as_deref() == Some("a2")
        ));
    }

    #[test]
    fn test_command_emitter_allocation_completion_no_command() {
        let mut emitter = super::CommandEmitter::new();

        // First: one allocation
        let desired1 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            function_call_results: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);

        // Second: allocation completed (removed from desired state)
        let desired2 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![],
            function_call_results: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        // Completed allocations disappear silently — no KillAllocation
        assert!(commands.is_empty(), "expected 0 commands: {commands:?}");
    }

    #[test]
    fn test_command_emitter_seq_continuity() {
        let mut emitter = super::CommandEmitter::new();

        // First batch: 2 commands (seq 1, 2)
        let desired1 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            function_call_results: vec![],
            clock: Some(1),
        };
        let cmds1 = emitter.emit_commands(&desired1);
        assert_eq!(cmds1.last().unwrap().seq, 2);

        // Second batch: 1 command (seq 3)
        let desired2 = executor_api_pb::DesiredExecutorState {
            function_executors: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
            function_call_results: vec![],
            clock: Some(2),
        };
        let cmds2 = emitter.emit_commands(&desired2);
        assert_eq!(cmds2[0].seq, 3, "seq should continue from previous batch");
    }
}
