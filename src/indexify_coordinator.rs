#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetContentMetadataRequest {
    #[prost(string, repeated, tag = "1")]
    pub content_list: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetContentMetadataResponse {
    #[prost(message, repeated, tag = "1")]
    pub content_list: ::prost::alloc::vec::Vec<ContentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTaskRequest {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    #[prost(enumeration = "TaskOutcome", tag = "3")]
    pub outcome: i32,
    #[prost(message, repeated, tag = "4")]
    pub content_list: ::prost::alloc::vec::Vec<ContentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdateTaskResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetExtractorCoordinatesRequest {
    #[prost(string, tag = "2")]
    pub extractor: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetExtractorCoordinatesResponse {
    #[prost(string, repeated, tag = "1")]
    pub addrs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesRequest {
    #[prost(string, tag = "1")]
    pub repository: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListIndexesResponse {
    #[prost(message, repeated, tag = "1")]
    pub indexes: ::prost::alloc::vec::Vec<Index>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexRequest {
    #[prost(string, tag = "1")]
    pub repository: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexResponse {
    #[prost(message, optional, tag = "1")]
    pub index: ::core::option::Option<Index>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexRequest {
    #[prost(message, optional, tag = "2")]
    pub index: ::core::option::Option<Index>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateIndexResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Index {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub repository: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub extractor_binding: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub extractor: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Embedding {
    #[prost(float, repeated, tag = "1")]
    pub embedding: ::prost::alloc::vec::Vec<f32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Attributes {
    #[prost(string, tag = "2")]
    pub attributes: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Feature {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub embedding: ::core::option::Option<Embedding>,
    #[prost(message, optional, tag = "3")]
    pub attributes: ::core::option::Option<Attributes>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Content {
    #[prost(string, tag = "2")]
    pub mime: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub features: ::prost::alloc::vec::Vec<Feature>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterExecutorRequest {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub addr: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub extractor: ::core::option::Option<Extractor>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterExecutorResponse {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
    #[prost(string, tag = "1")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub tasks: ::prost::alloc::vec::Vec<Task>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub extractor: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub repository: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub content_metadata: ::core::option::Option<ContentMetadata>,
    #[prost(string, tag = "5")]
    pub input_params: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub extractor_binding: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "7")]
    pub output_index_mapping: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(enumeration = "TaskOutcome", tag = "8")]
    pub outcome: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListExtractorsRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListExtractorsResponse {
    #[prost(message, repeated, tag = "1")]
    pub extractors: ::prost::alloc::vec::Vec<Extractor>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extractor {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub input_params: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "4")]
    pub outputs: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRepositoryRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetRepositoryResponse {
    #[prost(message, optional, tag = "1")]
    pub repository: ::core::option::Option<Repository>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListContentRequest {
    #[prost(string, tag = "1")]
    pub repository: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub source: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListContentResponse {
    #[prost(message, repeated, tag = "1")]
    pub content_list: ::prost::alloc::vec::Vec<ContentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListBindingsRequest {
    #[prost(string, tag = "1")]
    pub repository: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListBindingsResponse {
    #[prost(message, repeated, tag = "1")]
    pub bindings: ::prost::alloc::vec::Vec<ExtractorBinding>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRepositoryRequest {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub bindings: ::prost::alloc::vec::Vec<ExtractorBinding>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateRepositoryResponse {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int64, tag = "2")]
    pub created_at: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListRepositoriesRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListRepositoriesResponse {
    #[prost(message, repeated, tag = "1")]
    pub repositories: ::prost::alloc::vec::Vec<Repository>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatRepository {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub bindings: ::prost::alloc::vec::Vec<ExtractorBinding>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractorBinding {
    #[prost(string, tag = "1")]
    pub extractor: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub input_params: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "5")]
    pub filters: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(string, tag = "6")]
    pub content_source: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractorBindRequest {
    #[prost(string, tag = "1")]
    pub repository: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub binding: ::core::option::Option<ExtractorBinding>,
    #[prost(int64, tag = "2")]
    pub created_at: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractorBindResponse {
    #[prost(int64, tag = "3")]
    pub created_at: i64,
    #[prost(message, optional, tag = "2")]
    pub extractor: ::core::option::Option<Extractor>,
    #[prost(map = "string, string", tag = "4")]
    pub index_name_table_mapping: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(map = "string, string", tag = "5")]
    pub output_index_name_mapping: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContentMetadata {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub parent_id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub mime: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "5")]
    pub labels: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(string, tag = "6")]
    pub storage_url: ::prost::alloc::string::String,
    #[prost(int64, tag = "7")]
    pub created_at: i64,
    #[prost(string, tag = "8")]
    pub repository: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub source: ::prost::alloc::string::String,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateContentRequest {
    #[prost(message, optional, tag = "2")]
    pub content: ::core::option::Option<ContentMetadata>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateContentResponse {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Repository {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub bindings: ::prost::alloc::vec::Vec<ExtractorBinding>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TaskOutcome {
    Unknown = 0,
    Failed = 1,
    Success = 2,
}
impl TaskOutcome {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TaskOutcome::Unknown => "UNKNOWN",
            TaskOutcome::Failed => "FAILED",
            TaskOutcome::Success => "SUCCESS",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN" => Some(Self::Unknown),
            "FAILED" => Some(Self::Failed),
            "SUCCESS" => Some(Self::Success),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod coordinator_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CoordinatorServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CoordinatorServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CoordinatorServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CoordinatorServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CoordinatorServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn create_content(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateContentRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateContentResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/CreateContent",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "CreateContent",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_content_metadata(
            &mut self,
            request: impl tonic::IntoRequest<super::GetContentMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetContentMetadataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/GetContentMetadata",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "GetContentMetadata",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_content(
            &mut self,
            request: impl tonic::IntoRequest<super::ListContentRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListContentResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/ListContent",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "ListContent",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_binding(
            &mut self,
            request: impl tonic::IntoRequest<super::ExtractorBindRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ExtractorBindResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/CreateBinding",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "CreateBinding",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_bindings(
            &mut self,
            request: impl tonic::IntoRequest<super::ListBindingsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListBindingsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/ListBindings",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "ListBindings",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_repository(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateRepositoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateRepositoryResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/CreateRepository",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "CreateRepository",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_repositories(
            &mut self,
            request: impl tonic::IntoRequest<super::ListRepositoriesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListRepositoriesResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/ListRepositories",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "ListRepositories",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_repository(
            &mut self,
            request: impl tonic::IntoRequest<super::GetRepositoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetRepositoryResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/GetRepository",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "GetRepository",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn list_extractors(
            &mut self,
            request: impl tonic::IntoRequest<super::ListExtractorsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListExtractorsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/ListExtractors",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "ListExtractors",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn register_executor(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterExecutorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RegisterExecutorResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/RegisterExecutor",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "RegisterExecutor",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::HeartbeatRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::HeartbeatResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/Heartbeat",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "Heartbeat",
                    ),
                );
            self.inner.streaming(req, path, codec).await
        }
        pub async fn list_indexes(
            &mut self,
            request: impl tonic::IntoRequest<super::ListIndexesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/ListIndexes",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "ListIndexes",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_index(
            &mut self,
            request: impl tonic::IntoRequest<super::GetIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetIndexResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/GetIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "GetIndex",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn create_index(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/CreateIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "CreateIndex",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_extractor_coordinates(
            &mut self,
            request: impl tonic::IntoRequest<super::GetExtractorCoordinatesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetExtractorCoordinatesResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/GetExtractorCoordinates",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "GetExtractorCoordinates",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn update_task(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateTaskResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/indexify_coordinator.CoordinatorService/UpdateTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "indexify_coordinator.CoordinatorService",
                        "UpdateTask",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod coordinator_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CoordinatorServiceServer.
    #[async_trait]
    pub trait CoordinatorService: Send + Sync + 'static {
        async fn create_content(
            &self,
            request: tonic::Request<super::CreateContentRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateContentResponse>,
            tonic::Status,
        >;
        async fn get_content_metadata(
            &self,
            request: tonic::Request<super::GetContentMetadataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetContentMetadataResponse>,
            tonic::Status,
        >;
        async fn list_content(
            &self,
            request: tonic::Request<super::ListContentRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListContentResponse>,
            tonic::Status,
        >;
        async fn create_binding(
            &self,
            request: tonic::Request<super::ExtractorBindRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ExtractorBindResponse>,
            tonic::Status,
        >;
        async fn list_bindings(
            &self,
            request: tonic::Request<super::ListBindingsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListBindingsResponse>,
            tonic::Status,
        >;
        async fn create_repository(
            &self,
            request: tonic::Request<super::CreateRepositoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateRepositoryResponse>,
            tonic::Status,
        >;
        async fn list_repositories(
            &self,
            request: tonic::Request<super::ListRepositoriesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListRepositoriesResponse>,
            tonic::Status,
        >;
        async fn get_repository(
            &self,
            request: tonic::Request<super::GetRepositoryRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetRepositoryResponse>,
            tonic::Status,
        >;
        async fn list_extractors(
            &self,
            request: tonic::Request<super::ListExtractorsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListExtractorsResponse>,
            tonic::Status,
        >;
        async fn register_executor(
            &self,
            request: tonic::Request<super::RegisterExecutorRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RegisterExecutorResponse>,
            tonic::Status,
        >;
        /// Server streaming response type for the Heartbeat method.
        type HeartbeatStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::HeartbeatResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn heartbeat(
            &self,
            request: tonic::Request<tonic::Streaming<super::HeartbeatRequest>>,
        ) -> std::result::Result<tonic::Response<Self::HeartbeatStream>, tonic::Status>;
        async fn list_indexes(
            &self,
            request: tonic::Request<super::ListIndexesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListIndexesResponse>,
            tonic::Status,
        >;
        async fn get_index(
            &self,
            request: tonic::Request<super::GetIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetIndexResponse>,
            tonic::Status,
        >;
        async fn create_index(
            &self,
            request: tonic::Request<super::CreateIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateIndexResponse>,
            tonic::Status,
        >;
        async fn get_extractor_coordinates(
            &self,
            request: tonic::Request<super::GetExtractorCoordinatesRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetExtractorCoordinatesResponse>,
            tonic::Status,
        >;
        async fn update_task(
            &self,
            request: tonic::Request<super::UpdateTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::UpdateTaskResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CoordinatorServiceServer<T: CoordinatorService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CoordinatorService> CoordinatorServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CoordinatorServiceServer<T>
    where
        T: CoordinatorService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/indexify_coordinator.CoordinatorService/CreateContent" => {
                    #[allow(non_camel_case_types)]
                    struct CreateContentSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::CreateContentRequest>
                    for CreateContentSvc<T> {
                        type Response = super::CreateContentResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateContentRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::create_content(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateContentSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/GetContentMetadata" => {
                    #[allow(non_camel_case_types)]
                    struct GetContentMetadataSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::GetContentMetadataRequest>
                    for GetContentMetadataSvc<T> {
                        type Response = super::GetContentMetadataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetContentMetadataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::get_content_metadata(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetContentMetadataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/ListContent" => {
                    #[allow(non_camel_case_types)]
                    struct ListContentSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ListContentRequest>
                    for ListContentSvc<T> {
                        type Response = super::ListContentResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListContentRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::list_content(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListContentSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/CreateBinding" => {
                    #[allow(non_camel_case_types)]
                    struct CreateBindingSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ExtractorBindRequest>
                    for CreateBindingSvc<T> {
                        type Response = super::ExtractorBindResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExtractorBindRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::create_binding(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateBindingSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/ListBindings" => {
                    #[allow(non_camel_case_types)]
                    struct ListBindingsSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ListBindingsRequest>
                    for ListBindingsSvc<T> {
                        type Response = super::ListBindingsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListBindingsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::list_bindings(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListBindingsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/CreateRepository" => {
                    #[allow(non_camel_case_types)]
                    struct CreateRepositorySvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::CreateRepositoryRequest>
                    for CreateRepositorySvc<T> {
                        type Response = super::CreateRepositoryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateRepositoryRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::create_repository(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateRepositorySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/ListRepositories" => {
                    #[allow(non_camel_case_types)]
                    struct ListRepositoriesSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ListRepositoriesRequest>
                    for ListRepositoriesSvc<T> {
                        type Response = super::ListRepositoriesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListRepositoriesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::list_repositories(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListRepositoriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/GetRepository" => {
                    #[allow(non_camel_case_types)]
                    struct GetRepositorySvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::GetRepositoryRequest>
                    for GetRepositorySvc<T> {
                        type Response = super::GetRepositoryResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetRepositoryRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::get_repository(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetRepositorySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/ListExtractors" => {
                    #[allow(non_camel_case_types)]
                    struct ListExtractorsSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ListExtractorsRequest>
                    for ListExtractorsSvc<T> {
                        type Response = super::ListExtractorsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListExtractorsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::list_extractors(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListExtractorsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/RegisterExecutor" => {
                    #[allow(non_camel_case_types)]
                    struct RegisterExecutorSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::RegisterExecutorRequest>
                    for RegisterExecutorSvc<T> {
                        type Response = super::RegisterExecutorResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RegisterExecutorRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::register_executor(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RegisterExecutorSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/Heartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct HeartbeatSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::StreamingService<super::HeartbeatRequest>
                    for HeartbeatSvc<T> {
                        type Response = super::HeartbeatResponse;
                        type ResponseStream = T::HeartbeatStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::HeartbeatRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::heartbeat(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/ListIndexes" => {
                    #[allow(non_camel_case_types)]
                    struct ListIndexesSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::ListIndexesRequest>
                    for ListIndexesSvc<T> {
                        type Response = super::ListIndexesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListIndexesRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::list_indexes(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListIndexesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/GetIndex" => {
                    #[allow(non_camel_case_types)]
                    struct GetIndexSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::GetIndexRequest>
                    for GetIndexSvc<T> {
                        type Response = super::GetIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetIndexRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::get_index(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/CreateIndex" => {
                    #[allow(non_camel_case_types)]
                    struct CreateIndexSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::CreateIndexRequest>
                    for CreateIndexSvc<T> {
                        type Response = super::CreateIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateIndexRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::create_index(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/GetExtractorCoordinates" => {
                    #[allow(non_camel_case_types)]
                    struct GetExtractorCoordinatesSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::GetExtractorCoordinatesRequest>
                    for GetExtractorCoordinatesSvc<T> {
                        type Response = super::GetExtractorCoordinatesResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::GetExtractorCoordinatesRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::get_extractor_coordinates(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetExtractorCoordinatesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/indexify_coordinator.CoordinatorService/UpdateTask" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateTaskSvc<T: CoordinatorService>(pub Arc<T>);
                    impl<
                        T: CoordinatorService,
                    > tonic::server::UnaryService<super::UpdateTaskRequest>
                    for UpdateTaskSvc<T> {
                        type Response = super::UpdateTaskResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as CoordinatorService>::update_task(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateTaskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: CoordinatorService> Clone for CoordinatorServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: CoordinatorService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CoordinatorService> tonic::server::NamedService
    for CoordinatorServiceServer<T> {
        const NAME: &'static str = "indexify_coordinator.CoordinatorService";
    }
}
