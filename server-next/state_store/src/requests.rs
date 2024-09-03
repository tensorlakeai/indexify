pub enum RequestType {
    CreateNameSpace(NamespaceRequest),
}

pub struct NamespaceRequest {
    pub name: String,
}
