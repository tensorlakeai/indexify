use std::{env, path::PathBuf};

use anyhow::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=../../proto/executor_api.proto");
    println!("cargo:rerun-if-changed=../../proto/container_daemon.proto");

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

    Ok(())
}
