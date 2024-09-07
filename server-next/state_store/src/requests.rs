use data_model::{ComputeGraph, InvocationPayload, NodeOutput, Task};

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
    pub task_id: String,
    pub node_output: NodeOutput,
    pub task_outcome: data_model::TaskOutcome,
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
}

pub struct DeleteInvocationRequest {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}
