//! Generated protobuf definitions for the executor API, container daemon,
//! and function executor.
//!
//! This crate provides both client and server implementations for:
//! - Executor API: Used by both the server and dataplane executors
//! - Container Daemon API: Used for communication between dataplane and PID1
//!   daemon in containers
//! - Function Executor API: Used for communication between dataplane and
//!   function executor subprocesses

#[allow(non_camel_case_types)]
pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

#[allow(non_camel_case_types)]
pub mod container_daemon_pb {
    tonic::include_proto!("container_daemon_pb");
}

#[allow(non_camel_case_types)]
pub mod function_executor_pb {
    tonic::include_proto!("function_executor_service");
}

/// Re-export the google.rpc types (used by function_executor.proto).
#[allow(non_camel_case_types)]
pub mod google_rpc {
    tonic::include_proto!("google.rpc");
}

pub mod descriptor {
    pub const EXECUTOR_API_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("executor_api_descriptor");
    pub const CONTAINER_DAEMON_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("container_daemon_descriptor");
    pub const FUNCTION_EXECUTOR_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("function_executor_descriptor");
}
