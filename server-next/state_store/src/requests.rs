use data_model::{ComputeGraph, DataObject, Task};

pub enum RequestType {
    InvokeComputeGraph(InvokeComputeGraphRequest),
    CreateNameSpace(NamespaceRequest),
    CreateComputeGraph(CreateComputeGraphRequest),
    DeleteComputeGraph(DeleteComputeGraphRequest),
    CreateTasks(CreateTaskRequest),
}

pub struct InvokeComputeGraphRequest {
    pub namespace: String,
    pub compute_graph_name: String,
    pub data_object: DataObject,
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
