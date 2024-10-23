use data_model::{
    ComputeGraph, ExecutorId, ExecutorMetadata, GraphVersion, InvocationPayload, NodeOutput,
    ReduceTask, StateChangeId, Task, TaskDiagnostics, TaskId,
};

pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub state_changes_processed: Vec<StateChangeId>,
}

pub enum RequestPayload {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    RerunComputeGraph(RerunComputeGraphRequest),
    RerunInvocation(RerunInvocationRequest),
    FinalizeTask(FinalizeTaskRequest),
    CreateNameSpace(NamespaceRequest),
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteComputeGraph(DeleteComputeGraphRequest),
    DeleteInvocation(DeleteInvocationRequest),
    SchedulerUpdate(SchedulerUpdateRequest),
    RegisterExecutor(RegisterExecutorRequest),
    DeregisterExecutor(DeregisterExecutorRequest),
    RemoveGcUrls(Vec<String>),
    UpdateSystemTask(UpdateSystemTaskRequest),
    RemoveSystemTask(RemoveSystemTaskRequest),
}

#[derive(Debug, Clone)]
pub struct UpdateSystemTaskRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub restart_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RemoveSystemTaskRequest {
    pub namespace: String,
    pub compute_graph_name: String,
}

#[derive(Debug, Clone)]
pub struct RerunInvocationRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub graph_version: GraphVersion,
    pub invocation_id: String,
}

#[derive(Debug, Clone)]
pub struct FinalizeTaskRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub node_outputs: Vec<NodeOutput>,
    pub task_outcome: data_model::TaskOutcome,
    pub executor_id: ExecutorId,
    pub diagnostics: Option<TaskDiagnostics>,
}

pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
}

#[derive(Debug, Clone)]
pub struct RerunComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
}

pub struct NamespaceRequest {
    pub name: String,
}

pub struct CreateComputeGraphRequest {
    pub namespace: String,
    pub compute_graph: ComputeGraph,
}

pub struct DeleteComputeGraphRequest {
    pub namespace: String,
    pub name: String,
}

pub struct DeleteComputeGraphOutputRequest {
    pub key: String,
    pub restart_key: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct CreateTasksRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub tasks: Vec<Task>,
}

#[derive(Debug)]
pub struct TaskPlacement {
    pub task: Task,
    pub executor: ExecutorId,
}

#[derive(Default, Debug)]
pub struct ReductionTasks {
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
}
#[derive(Debug)]
pub struct SchedulerUpdateRequest {
    pub task_requests: Vec<CreateTasksRequest>,
    pub allocations: Vec<TaskPlacement>,
    pub reduction_tasks: ReductionTasks,
    pub diagnostic_msgs: Vec<String>,
}

pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}

pub struct RegisterExecutorRequest {
    pub executor: ExecutorMetadata,
}

pub struct DeregisterExecutorRequest {
    pub executor_id: ExecutorId,
}
