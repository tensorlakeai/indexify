//! Generated protobuf definitions for the executor API and container daemon.
//!
//! This crate provides both client and server implementations for:
//! - Executor API: Used by both the server and dataplane executors
//! - Container Daemon API: Used for communication between dataplane and PID1
//!   daemon in containers

#[allow(non_camel_case_types)]
pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

#[allow(non_camel_case_types)]
pub mod container_daemon_pb {
    tonic::include_proto!("container_daemon_pb");
}

pub mod descriptor {
    pub const EXECUTOR_API_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("executor_api_descriptor");
    pub const CONTAINER_DAEMON_FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("container_daemon_descriptor");
}
