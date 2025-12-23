//! Generated protobuf definitions for the executor API.
//!
//! This crate provides both client and server implementations for the executor
//! API, allowing it to be used by both the server and dataplane executors.

#[allow(non_camel_case_types)]
pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

pub mod descriptor {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("executor_api_descriptor");
}
