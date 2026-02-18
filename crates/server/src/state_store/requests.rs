use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::AtomicU64},
};

use anyhow::Result;
use tracing::info;

use crate::{
    data_model::{
        Allocation,
        Application,
        ComputeOp,
        ContainerId,
        ContainerPool,
        ContainerPoolId,
        ContainerPoolKey,
        ContainerServerMetadata,
        DataPayload,
        DataplaneResultsIngestedEvent,
        ExecutorId,
        ExecutorMetadata,
        ExecutorServerMetadata,
        FunctionCall,
        FunctionCallId,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        RequestCtx,
        Sandbox,
        SandboxId,
        SandboxKey,
        StateChange,
    },
    state_store::{IndexifyState, executor_watches::ExecutorWatch, state_changes},
};

#[derive(Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
}

impl StateMachineUpdateRequest {
    /// Prepares all objects in the payload with clock values.
    /// This should be called once before persisting or storing in memory.
    /// After this call, both the persistent store and in-memory store
    /// receive the same prepared objects.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        match &mut self.payload {
            RequestPayload::InvokeApplication(req) => {
                req.ctx.prepare_for_persistence(clock);
            }
            RequestPayload::SchedulerUpdate(payload) => {
                for request_ctx in payload.update.updated_request_states.values_mut() {
                    request_ctx.prepare_for_persistence(clock);
                }
            }
            // Other payload types don't have objects with clocks that need preparation,
            // or they're handled directly in their respective state machine functions.
            _ => {}
        }
    }

    pub fn state_changes(
        &self,
        state_change_id_seq: &AtomicU64,
    ) -> anyhow::Result<Vec<StateChange>> {
        match &self.payload {
            RequestPayload::InvokeApplication(request) => {
                state_changes::invoke_application(state_change_id_seq, request)
            }
            RequestPayload::CreateFunctionCall(request) => {
                state_changes::create_function_call(state_change_id_seq, request)
            }
            RequestPayload::TombstoneApplication(request) => {
                state_changes::tombstone_application(state_change_id_seq, request)
            }
            RequestPayload::TombstoneRequest(request) => {
                state_changes::tombstone_request(state_change_id_seq, request)
            }
            RequestPayload::SchedulerUpdate(payload) => {
                Ok(payload.update.new_state_changes.clone())
            }
            RequestPayload::DeregisterExecutor(request) => Ok(request.state_changes.clone()),
            RequestPayload::UpsertExecutor(request) => Ok(request.state_changes.clone()),
            RequestPayload::CreateSandbox(request) => {
                state_changes::create_sandbox(state_change_id_seq, request)
            }
            RequestPayload::TerminateSandbox(request) => {
                state_changes::terminate_sandbox(state_change_id_seq, request)
            }
            RequestPayload::CreateContainerPool(request) => {
                state_changes::create_container_pool(state_change_id_seq, request)
            }
            RequestPayload::UpdateContainerPool(request) => {
                state_changes::update_container_pool(state_change_id_seq, request)
            }
            RequestPayload::TombstoneContainerPool(request) => {
                state_changes::delete_container_pool(state_change_id_seq, request)
            }
            RequestPayload::CreateOrUpdateApplication(request) => {
                state_changes::create_or_update_application_pools(state_change_id_seq, request)
            }
            RequestPayload::DataplaneResults(request) => {
                state_changes::dataplane_results_ingested(state_change_id_seq, request)
            }
            _ => Ok(Vec::new()), // Handle other request types as needed
        }
    }
}

#[derive(Debug, Clone, strum::Display)]
pub enum RequestPayload {
    InvokeApplication(InvokeApplicationRequest),
    CreateFunctionCall(FunctionCallRequest),
    CreateNameSpace(NamespaceRequest),
    CreateOrUpdateApplication(Box<CreateOrUpdateApplicationRequest>),
    TombstoneApplication(DeleteApplicationRequest),
    TombstoneRequest(DeleteRequestRequest),
    UpsertExecutor(UpsertExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    CreateSandbox(CreateSandboxRequest),
    TerminateSandbox(TerminateSandboxRequest),
    CreateContainerPool(CreateContainerPoolRequest),
    UpdateContainerPool(UpdateContainerPoolRequest),

    /// Add a single watch for an executor (from AllocationEvents RPC).
    AddExecutorWatch(AddExecutorWatchRequest),
    /// Remove a single watch for an executor (from AllocationEvents RPC).
    RemoveExecutorWatch(RemoveExecutorWatchRequest),

    /// Dataplane reports allocation results + container state changes
    /// atomically.
    DataplaneResults(DataplaneResultsRequest),

    // App Processor -> State Machine requests
    SchedulerUpdate(SchedulerUpdatePayload),
    DeleteApplicationRequest((DeleteApplicationRequest, Vec<StateChange>)),
    DeleteRequestRequest((DeleteRequestRequest, Vec<StateChange>)),
    TombstoneContainerPool(DeleteContainerPoolRequest),
    DeleteContainerPool((DeleteContainerPoolRequest, Vec<StateChange>)),
    ProcessStateChanges(Vec<StateChange>),
}

/// Wraps a SchedulerUpdateRequest together with the state changes it processed.
///
/// `update` contains the mutations to apply (new allocations, updated runs,
/// etc.) along with any *new* state changes produced during processing.
///
/// `processed_state_changes` are the input state changes that were consumed to
/// produce this update — they get marked as processed in the persistent store.
#[derive(Debug, Clone)]
pub struct SchedulerUpdatePayload {
    pub update: Box<SchedulerUpdateRequest>,
    pub processed_state_changes: Vec<StateChange>,
}

impl SchedulerUpdatePayload {
    /// Creates a payload with no processed state changes (used for intermediate
    /// scheduler updates that don't consume state changes from the queue).
    pub fn new(update: SchedulerUpdateRequest) -> Self {
        Self {
            update: Box::new(update),
            processed_state_changes: vec![],
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchedulerUpdateRequest {
    pub new_allocations: Vec<Allocation>,
    pub updated_allocations: Vec<Allocation>,
    pub updated_function_runs: HashMap<String, HashSet<FunctionCallId>>,
    pub updated_function_calls: HashMap<String, HashSet<FunctionCallId>>,
    pub updated_request_states: HashMap<String, RequestCtx>,
    pub remove_executors: Vec<ExecutorId>,
    pub updated_executor_states: HashMap<ExecutorId, Box<ExecutorServerMetadata>>,
    pub containers: HashMap<ContainerId, Box<ContainerServerMetadata>>,
    pub new_state_changes: Vec<StateChange>,
    pub updated_sandboxes: HashMap<SandboxKey, Sandbox>,
    pub updated_pools: HashMap<ContainerPoolKey, ContainerPool>,
    pub deleted_pools: HashSet<ContainerPoolKey>,
    pub function_pool_deficits: Option<super::in_memory_state::ResourceProfileHistogram>,
    pub sandbox_pool_deficits: Option<super::in_memory_state::ResourceProfileHistogram>,
    /// Pools that the buffer reconciler found unsatisfiable (no resources).
    /// Propagated to the real scheduler's blocked_pools for cross-cycle
    /// persistence so these pools are skipped until resources become available.
    pub newly_blocked_pools: HashSet<ContainerPoolKey>,
}

impl SchedulerUpdateRequest {
    /// Extends this SchedulerUpdateRequest with contents from another one
    pub fn extend(&mut self, other: SchedulerUpdateRequest) {
        self.new_allocations.extend(other.new_allocations);
        self.updated_allocations.extend(other.updated_allocations);
        for (ctx_key, function_run_ids) in other.updated_function_runs {
            self.updated_function_runs
                .entry(ctx_key)
                .or_default()
                .extend(function_run_ids);
        }
        for (ctx_key, function_call_ids) in other.updated_function_calls {
            self.updated_function_calls
                .entry(ctx_key)
                .or_default()
                .extend(function_call_ids);
        }
        self.updated_request_states
            .extend(other.updated_request_states);
        self.new_state_changes.extend(other.new_state_changes);

        self.remove_executors.extend(other.remove_executors);
        for (executor_id, executor_server_metadata) in other.updated_executor_states {
            self.updated_executor_states
                .insert(executor_id, executor_server_metadata);
        }
        self.containers.extend(other.containers);
        self.updated_sandboxes.extend(other.updated_sandboxes);
        self.updated_pools.extend(other.updated_pools);
        self.deleted_pools.extend(other.deleted_pools);
        if other.function_pool_deficits.is_some() {
            self.function_pool_deficits = other.function_pool_deficits;
        }
        if other.sandbox_pool_deficits.is_some() {
            self.sandbox_pool_deficits = other.sandbox_pool_deficits;
        }
        self.newly_blocked_pools.extend(other.newly_blocked_pools);
    }

    pub fn cancel_allocation(&mut self, allocation: &mut Allocation) {
        info!(
            allocation_id = %allocation.id,
            request_id = %allocation.request_id,
            namespace = %allocation.namespace,
            app = %allocation.application,
            fn = %allocation.function,
            fn_executor_id = %allocation.target.function_executor_id,
            "cancelling allocation",
        );
        allocation.outcome =
            FunctionRunOutcome::Failure(FunctionRunFailureReason::FunctionRunCancelled);
        self.updated_allocations.push(allocation.clone());
    }

    /// Adds a function run to the request context and tracks it as updated.
    ///
    /// NOTE: This does NOT snapshot the RequestCtx. Callers MUST call
    /// `add_request_state()` once before returning the SchedulerUpdateRequest
    /// or before passing it to `in_memory_state.update_state()`.
    /// This avoids O(N^2) cloning when adding many function runs to the same
    /// request context (e.g., during 1000-item map-reduce).
    pub fn add_function_run(&mut self, function_run: FunctionRun, request_ctx: &mut RequestCtx) {
        request_ctx
            .function_runs
            .insert(function_run.id.clone(), function_run.clone());
        self.updated_function_runs
            .entry(request_ctx.key())
            .or_default()
            .insert(function_run.id.clone());
    }

    /// Snapshots the current state of a RequestCtx into the update.
    /// Call this once after all mutations are done, before the scheduler update
    /// is consumed by `in_memory_state.update_state()` or persisted.
    pub fn add_request_state(&mut self, request_ctx: &RequestCtx) {
        self.updated_request_states
            .insert(request_ctx.key(), request_ctx.clone());
    }

    /// Adds a function call to the request context and tracks it as updated.
    ///
    /// NOTE: This does NOT snapshot the RequestCtx.
    pub fn add_function_call(&mut self, function_call: FunctionCall, request_ctx: &mut RequestCtx) {
        let fc_id = function_call.function_call_id.clone();
        request_ctx
            .function_calls
            .insert(fc_id.clone(), function_call);
        self.updated_function_calls
            .entry(request_ctx.key())
            .or_default()
            .insert(fc_id);
    }

    pub fn unallocated_function_runs(&self) -> Vec<FunctionRun> {
        let mut function_runs = Vec::new();
        for (request_ctx_key, function_call_ids) in &self.updated_function_runs {
            if let Some(request_ctx) = self.updated_request_states.get(request_ctx_key) {
                for function_call_id in function_call_ids {
                    if let Some(function_run) = request_ctx.function_runs.get(function_call_id) &&
                        matches!(function_run.status, FunctionRunStatus::Pending)
                    {
                        function_runs.push(function_run.clone());
                    }
                }
            }
        }
        function_runs
    }
}

#[derive(Debug, Clone)]
pub struct RequestUpdates {
    pub request_updates: Vec<ComputeOp>,
    // The function call id which is the root of the call graph of the functions
    // calls
    pub output_function_call_id: FunctionCallId,
}

#[derive(Debug, Clone)]
pub struct AllocationOutput {
    pub request_id: String,
    pub allocation: Allocation,
    pub data_payload: Option<DataPayload>,
    pub executor_id: ExecutorId,
    pub request_exception: Option<DataPayload>,
    pub graph_updates: Option<RequestUpdates>,
}

#[derive(Debug, Clone)]
pub struct InvokeApplicationRequest {
    pub namespace: String,
    pub application_name: String,
    pub ctx: RequestCtx,
}

#[derive(Debug, Clone)]
pub struct FunctionCallRequest {
    pub namespace: String,
    pub application_name: String,
    pub request_id: String,
    pub graph_updates: RequestUpdates,
    pub source_function_call_id: FunctionCallId,
}

#[derive(Debug, Clone)]
pub struct NamespaceRequest {
    pub name: String,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateOrUpdateApplicationRequest {
    pub namespace: String,
    pub application: Application,
    pub upgrade_requests_to_current_version: bool,
    pub container_pools: Vec<ContainerPool>,
}

#[derive(Debug, Clone)]
pub struct DeleteApplicationRequest {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DeleteRequestRequest {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
}

/// Request to upsert an executor, including its metadata and diagnostics.
/// **DO NOT** construct this directly, use `UpsertExecutorRequest::build`
/// instead.
#[derive(Debug, Clone)]
pub struct UpsertExecutorRequest {
    pub executor: ExecutorMetadata,
    pub allocation_outputs: Vec<AllocationOutput>,
    pub update_executor_state: bool,
    pub watch_function_calls: HashSet<ExecutorWatch>,
    state_changes: Vec<StateChange>,
}

impl UpsertExecutorRequest {
    /// Builds a new UpsertExecutorRequest.
    /// This function will also generate the state changes
    /// needed to update the executor state in the indexify state.
    ///
    /// It will also check if the allocation outputs can be updated
    /// and generate the necessary state changes
    /// for the allocation outputs.
    pub fn build(
        executor: ExecutorMetadata,
        allocation_outputs: Vec<AllocationOutput>,
        update_executor_state: bool,
        watch_function_calls: HashSet<ExecutorWatch>,
        indexify_state: Arc<IndexifyState>,
    ) -> Result<Self> {
        let mut state_changes = Vec::new();

        if update_executor_state {
            let changes =
                state_changes::upsert_executor(&indexify_state.state_change_id_seq, &executor.id)?;
            state_changes = changes;
        }

        Ok(Self {
            executor,
            allocation_outputs,
            state_changes,
            update_executor_state,
            watch_function_calls,
        })
    }
}

/// Add a single watch for an executor (from AllocationEvents RPC).
#[derive(Debug, Clone)]
pub struct AddExecutorWatchRequest {
    pub executor_id: ExecutorId,
    pub watch: ExecutorWatch,
}

/// Remove a single watch for an executor (from AllocationEvents RPC).
#[derive(Debug, Clone)]
pub struct RemoveExecutorWatchRequest {
    pub executor_id: ExecutorId,
    pub watch: ExecutorWatch,
}

#[derive(Debug, Clone)]
pub struct DeregisterExecutorRequest {
    pub executor_id: ExecutorId,
    pub state_changes: Vec<StateChange>,
}

#[derive(Debug, Clone)]
pub struct CreateSandboxRequest {
    pub sandbox: Sandbox,
}

#[derive(Debug, Clone)]
pub struct TerminateSandboxRequest {
    pub namespace: String,
    pub sandbox_id: SandboxId,
}

#[derive(Debug, Clone)]
pub struct CreateContainerPoolRequest {
    pub pool: ContainerPool,
}

#[derive(Debug, Clone)]
pub struct UpdateContainerPoolRequest {
    pub pool: ContainerPool,
}

#[derive(Debug, Clone)]
pub struct DeleteContainerPoolRequest {
    pub namespace: String,
    pub pool_id: ContainerPoolId,
}

#[derive(Debug, Clone)]
pub struct DataplaneResultsRequest {
    pub event: DataplaneResultsIngestedEvent,
}
