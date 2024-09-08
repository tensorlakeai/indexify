use data_model::{
    ComputeGraph,
    ExecutorId,
    InvocationPayload,
    NodeOutput,
    StateChangeId,
    Task,
    TaskId,
};

pub enum RequestType {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    FinalizeTask(FinalizeTaskRequest),
    CreateNameSpace(NamespaceRequest),
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteComputeGraph(DeleteComputeGraphRequest),
    CreateTasks(CreateTaskRequest),
    DeleteInvocation(DeleteInvocationRequest),
}

pub struct FinalizeTaskRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub node_outputs: Vec<NodeOutput>,
    pub task_outcome: data_model::TaskOutcome,
    pub executor_id: ExecutorId,
}

pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_payload: InvocationPayload,
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

pub struct CreateTaskRequest {
    pub tasks: Vec<Task>,
    pub processed_state_changes: Vec<StateChangeId>,
}

pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}
