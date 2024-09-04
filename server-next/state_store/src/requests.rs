use data_model::ComputeGraph;

pub enum RequestType {
    CreateNameSpace(NamespaceRequest),
    CreateComputeGraph(ComputeGraph),
}

pub struct NamespaceRequest {
    pub name: String,
}
