//! Indexify Dataplane Library
//!
//! This module exposes the dataplane components for testing and reuse.

pub mod config;
pub mod daemon_binary;
pub mod daemon_client;
pub mod driver;
pub mod function_container_manager;
pub mod http_proxy;
pub mod metrics;
pub mod network_rules;
mod resources;
pub mod state_file;

// Re-export key types for convenience
pub use daemon_client::DaemonClient;
pub use driver::{DockerDriver, ForkExecDriver, ProcessConfig, ProcessDriver, ProcessHandle};
pub use function_container_manager::{
    DefaultImageResolver,
    FunctionContainerManager,
    ImageResolver,
    SandboxLookupResult,
};
pub use http_proxy::run_http_proxy;
pub use metrics::{ContainerCounts, DataplaneMetrics, ResourceAvailability};
