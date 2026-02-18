//! Indexify Dataplane Library
//!
//! This module exposes the dataplane components for testing and reuse.

pub mod allocation_controller;
pub mod blob_ops;
pub mod code_cache;
pub mod config;
pub mod daemon_binary;
pub mod daemon_client;
pub mod driver;
pub mod function_container_manager;
pub mod function_executor;
pub mod gpu_allocator;
pub mod grpc;
pub mod http_proxy;
pub mod metrics;
pub mod monitoring;
pub mod network_rules;
pub mod otel_tracing;
pub mod resources;
pub mod retry;
pub mod secrets;
pub mod service;
pub mod state_file;
pub mod state_reconciler;
pub mod state_reporter;
pub mod validation;

// Re-export key types for convenience
pub use daemon_client::DaemonClient;
pub use driver::{
    DockerDriver,
    ForkExecDriver,
    ProcessConfig,
    ProcessDriver,
    ProcessHandle,
    ProcessType,
};
pub use function_container_manager::{
    DefaultImageResolver,
    FunctionContainerManager,
    ImageResolver,
    SandboxLookupResult,
};
pub use http_proxy::run_http_proxy;
pub use metrics::{ContainerCounts, DataplaneMetrics, ResourceAvailability};
pub use secrets::{NoopSecretsProvider, SecretsProvider};
pub use service::Service;
