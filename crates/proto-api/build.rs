use std::{env, path::PathBuf};

use anyhow::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=../../proto/executor_api.proto");
    println!("cargo:rerun-if-changed=../../proto/container_daemon.proto");
    println!("cargo:rerun-if-changed=../../proto/function_executor.proto");
    println!("cargo:rerun-if-changed=../../proto/status.proto");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Compile executor_api.proto
    let executor_api_files = ["../../proto/executor_api.proto"];
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("executor_api_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&executor_api_files, &["../../proto"])?;

    // Compile container_daemon.proto
    let container_daemon_files = ["../../proto/container_daemon.proto"];
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("container_daemon_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&container_daemon_files, &["../../proto"])?;

    // First compile status.proto (google.rpc.Status) standalone so we control its
    // path.
    let status_files = ["../../proto/status.proto"];
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&status_files, &["../../proto"])?;

    // Compile function_executor.proto (client only — we call the FE, don't serve
    // it). Map google.rpc types to our google_rpc module so generated code can
    // find them.
    let function_executor_files = ["../../proto/function_executor.proto"];
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .file_descriptor_set_path(out_dir.join("function_executor_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".google.rpc", "crate::google_rpc")
        .compile_protos(&function_executor_files, &["../../proto"])?;

    Ok(())
}
