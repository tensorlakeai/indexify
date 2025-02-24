use data_model::{
    Allocation,
    ComputeGraph,
    ExecutorId,
    ExecutorMetadata,
    GraphInvocationCtx,
    InvocationPayload,
    NodeOutput,
    ReduceTask,
    StateChange,
    Task,
    TaskDiagnostics,
    TaskId,
    TaskOutcome,
};

pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub processed_state_changes: Vec<StateChange>,
}

#[derive(Debug, Clone, strum::Display)]
pub enum RequestPayload {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    IngestTaskOutputs(IngestTaskOutputsRequest),
    CreateNameSpace(NamespaceRequest),
    CreateOrUpdateComputeGraph(CreateOrUpdateComputeGraphRequest),
    TombstoneComputeGraph(DeleteComputeGraphRequest),
    TombstoneInvocation(DeleteInvocationRequest),
    SchedulerUpdate(SchedulerUpdateRequest),
    NamespaceProcessorUpdate(NamespaceProcessorUpdateRequest),
    TaskAllocationProcessorUpdate(TaskAllocationUpdateRequest),
    RegisterExecutor(RegisterExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    RemoveGcUrls(Vec<String>),
    MutateClusterTopology(MutateClusterTopologyRequest),
    DeleteComputeGraphRequest(DeleteComputeGraphRequest),
    DeleteInvocationRequest(DeleteInvocationRequest),
    Noop,
}

#[derive(Debug, Clone)]
pub struct SchedulerUpdateRequest {
    pub new_allocations: Vec<Allocation>,
    pub remove_allocations: Vec<Allocation>,
    pub updated_tasks: Vec<Task>,
    pub updated_invocations_states: Vec<GraphInvocationCtx>,
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MutateClusterTopologyRequest {
    pub executor_removed: ExecutorId,
}

#[derive(Debug, Clone)]
pub struct IngestTaskOutputsRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task: Task,
    pub node_outputs: Vec<NodeOutput>,
    pub task_outcome: TaskOutcome,
    pub diagnostics: Option<TaskDiagnostics>,
    pub executor_id: ExecutorId,
}

#[derive(Debug, Clone)]
pub struct FinalizeTaskRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub task_outcome: TaskOutcome,
    pub diagnostics: Option<TaskDiagnostics>,
    pub executor_id: ExecutorId,
    pub invocation_ctx: GraphInvocationCtx,
}

#[derive(Debug, Clone)]
pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
    pub ctx: GraphInvocationCtx,
}

#[derive(Debug, Clone)]
pub struct ReplayComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
}

#[derive(Debug, Clone)]
pub struct NamespaceRequest {
    pub name: String,
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

#[derive(Debug, Clone)]
pub struct DeleteComputeGraphOutputRequest {
    pub key: String,
    pub restart_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TaskPlacement {
    pub task: Task,
    pub executor: ExecutorId,
}

#[derive(Debug, Clone)]
pub struct TaskPlacementDiagnostic {
    pub task: Task,
    pub message: String,
}

#[derive(Debug, Clone, Default)]
pub struct ReductionTasks {
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
}
#[derive(Debug, Clone)]
pub struct NamespaceProcessorUpdateRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub task_requests: Vec<Task>,
    pub invocation_ctx: Option<GraphInvocationCtx>,
    pub reduction_tasks: ReductionTasks,
}

#[derive(Debug, Clone)]
pub struct TaskAllocationUpdateRequest {
    pub new_allocations: Vec<Allocation>,
    pub remove_allocations: Vec<Allocation>,
    pub updated_tasks: Vec<Task>,
}

#[derive(Debug, Clone)]
pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}

#[derive(Debug, Clone)]
pub struct RegisterExecutorRequest {
    pub executor: ExecutorMetadata,
}

#[derive(Debug, Clone)]
pub struct DeregisterExecutorRequest {
    pub executor_id: ExecutorId,
}
