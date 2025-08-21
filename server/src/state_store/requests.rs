use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU64, Arc},
};

use anyhow::Result;

use crate::{
    data_model::{
        Allocation,
        ComputeGraph,
        ExecutorId,
        ExecutorMetadata,
        FunctionExecutorDiagnostics,
        FunctionExecutorId,
        FunctionExecutorServerMetadata,
        GcUrl,
        GraphInvocationCtx,
        HostResources,
        InvocationPayload,
        NodeOutput,
        ReduceTask,
        StateChange,
        Task,
        TaskId,
    },
    state_store::{state_changes, IndexifyState},
};

#[derive(Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
}

impl StateMachineUpdateRequest {
    pub fn state_changes(
        &self,
        state_change_id_seq: &AtomicU64,
    ) -> anyhow::Result<Vec<StateChange>> {
        match &self.payload {
            RequestPayload::InvokeComputeGraph(request) => {
                state_changes::invoke_compute_graph(state_change_id_seq, request)
            }
            RequestPayload::TombstoneComputeGraph(request) => {
                state_changes::tombstone_compute_graph(state_change_id_seq, request)
            }
            RequestPayload::TombstoneInvocation(request) => {
                state_changes::tombstone_invocation(state_change_id_seq, request)
            }
            RequestPayload::DeregisterExecutor(request) => {
                state_changes::tombstone_executor(state_change_id_seq, request)
            }
            RequestPayload::SchedulerUpdate((request, _)) => Ok(request.state_changes.clone()),
            RequestPayload::UpsertExecutor(request) => Ok(request.state_changes.clone()),
            _ => Ok(Vec::new()), // Handle other request types as needed
        }
    }
}

#[derive(Debug, Clone, strum::Display)]
pub enum RequestPayload {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    CreateNameSpace(NamespaceRequest),
    CreateOrUpdateComputeGraph(CreateOrUpdateComputeGraphRequest),
    TombstoneComputeGraph(DeleteComputeGraphRequest),
    TombstoneInvocation(DeleteInvocationRequest),
    SchedulerUpdate((Box<SchedulerUpdateRequest>, Vec<StateChange>)),
    UpsertExecutor(UpsertExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    RemoveGcUrls(Vec<GcUrl>),
    DeleteComputeGraphRequest((DeleteComputeGraphRequest, Vec<StateChange>)),
    DeleteInvocationRequest((DeleteInvocationRequest, Vec<StateChange>)),
    ProcessStateChanges(Vec<StateChange>),
}

#[derive(Debug, Clone, Default)]
pub struct SchedulerUpdateRequest {
    pub new_allocations: Vec<Allocation>,
    pub updated_tasks: HashMap<TaskId, Task>,
    pub cached_task_keys: HashSet<String>,
    pub updated_invocations_states: Vec<GraphInvocationCtx>,
    pub reduction_tasks: ReductionTasks,
    pub remove_executors: Vec<ExecutorId>,
    pub new_function_executors: Vec<FunctionExecutorServerMetadata>,
    pub remove_function_executors: HashMap<ExecutorId, HashSet<FunctionExecutorId>>,
    pub updated_executor_resources: HashMap<ExecutorId, HostResources>,
    pub state_changes: Vec<StateChange>,
}

impl SchedulerUpdateRequest {
    /// Extends this SchedulerUpdateRequest with contents from another one
    pub fn extend(&mut self, other: SchedulerUpdateRequest) {
        self.new_allocations.extend(other.new_allocations);
        self.updated_tasks.extend(other.updated_tasks);
        self.cached_task_keys.extend(other.cached_task_keys);
        self.updated_invocations_states
            .extend(other.updated_invocations_states);
        self.state_changes.extend(other.state_changes);

        self.reduction_tasks
            .new_reduction_tasks
            .extend(other.reduction_tasks.new_reduction_tasks);
        self.reduction_tasks
            .processed_reduction_tasks
            .extend(other.reduction_tasks.processed_reduction_tasks);

        self.remove_executors.extend(other.remove_executors);
        self.new_function_executors
            .extend(other.new_function_executors);
        self.remove_function_executors
            .extend(other.remove_function_executors);
        self.updated_executor_resources
            .extend(other.updated_executor_resources);
    }
}

#[derive(Debug, Clone)]
pub struct AllocationOutput {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub allocation: Allocation,
    pub node_output: NodeOutput,
    pub executor_id: ExecutorId,
    pub allocation_key: String,
}

#[derive(Debug, Clone)]
pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
    pub ctx: GraphInvocationCtx,
}

#[derive(Debug, Clone)]
pub struct NamespaceRequest {
    pub name: String,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreateOrUpdateComputeGraphRequest {
    pub namespace: String,
    pub compute_graph: ComputeGraph,
    pub upgrade_tasks_to_current_version: bool,
}

#[derive(Debug, Clone)]
pub struct DeleteComputeGraphRequest {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct ReductionTasks {
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}

/// Request to upsert an executor, including its metadata and diagnostics.
/// **DO NOT** construct this directly, use `UpsertExecutorRequest::build`
/// instead.
#[derive(Debug, Clone)]
pub struct UpsertExecutorRequest {
    pub executor: ExecutorMetadata,
    pub function_executor_diagnostics: Vec<FunctionExecutorDiagnostics>,
    pub allocation_outputs: Vec<AllocationOutput>,
    pub update_executor_state: bool,
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
        function_executor_diagnostics: Vec<FunctionExecutorDiagnostics>,
        allocation_outputs: Vec<AllocationOutput>,
        update_executor_state: bool,
        indexify_state: Arc<IndexifyState>,
    ) -> Result<Self> {
        let state_change_id_seq = indexify_state.state_change_id_seq();
        let mut state_changes = Vec::new();

        if update_executor_state {
            let changes = state_changes::upsert_executor(&state_change_id_seq, &executor.id)?;
            state_changes = changes;
        }

        for allocation_output in &allocation_outputs {
            if indexify_state.can_allocation_output_be_updated(&allocation_output)? {
                let changes =
                    state_changes::task_outputs_ingested(&state_change_id_seq, &allocation_output)?;
                state_changes.extend(changes);
            }
        }

        Ok(Self {
            executor,
            function_executor_diagnostics,
            allocation_outputs,
            state_changes,
            update_executor_state,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DeregisterExecutorRequest {
    pub executor_id: ExecutorId,
}
