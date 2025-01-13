use data_model::{
    ComputeGraph,
    ExecutorId,
    ExecutorMetadata,
    GraphVersion,
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
    ReplayComputeGraph(ReplayComputeGraphRequest),
    ReplayInvocations(ReplayInvocationsRequest),
    FinalizeTask(FinalizeTaskRequest),
    CreateNameSpace(NamespaceRequest),
    CreateOrUpdateComputeGraph(CreateOrUpdateComputeGraphRequest),
    TombstoneComputeGraph(DeleteComputeGraphRequest),
    TombstoneInvocation(DeleteInvocationRequest),
    NamespaceProcessorUpdate(NamespaceProcessorUpdateRequest),
    TaskAllocationProcessorUpdate(TaskAllocationUpdateRequest),
    RegisterExecutor(RegisterExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    RemoveGcUrls(Vec<String>),
    UpdateSystemTask(UpdateSystemTaskRequest),
    RemoveSystemTask(RemoveSystemTaskRequest),
    MutateClusterTopology(MutateClusterTopologyRequest),
    DeleteComputeGraphRequest(DeleteComputeGraphRequest),
    DeleteInvocationRequest(DeleteInvocationRequest),
    Noop,
}

#[derive(Debug, Clone)]
pub struct MutateClusterTopologyRequest {
    pub executor_removed: ExecutorId,
}

#[derive(Debug, Clone)]
pub struct UpdateSystemTaskRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub waiting_for_running_invocations: bool,
}

#[derive(Debug, Clone)]
pub struct RemoveSystemTaskRequest {
    pub namespace: String,
    pub compute_graph_name: String,
}

#[derive(Debug, Clone)]
pub struct ReplayInvocationsRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub graph_version: GraphVersion,
    pub invocation_ids: Vec<String>,
    pub restart_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FinalizeTaskRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub node_output: Option<NodeOutput>,
    pub task_outcome: TaskOutcome,
    pub executor_id: ExecutorId,
    pub diagnostics: Option<TaskDiagnostics>,
}

#[derive(Debug, Clone)]
pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
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
    pub reduction_tasks: ReductionTasks,
}

#[derive(Debug, Clone)]
pub struct TaskAllocationUpdateRequest {
    pub allocations: Vec<TaskPlacement>,
    pub unplaced_task_keys: Vec<String>,
    pub placement_diagnostics: Vec<TaskPlacementDiagnostic>,
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
