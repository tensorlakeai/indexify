use std::{env, path::PathBuf};

use anyhow::Result;
use vergen::{BuildBuilder, Emitter, SysinfoBuilder};

fn main() -> Result<()> {
    let build = BuildBuilder::all_build()?;
    let si = SysinfoBuilder::all_sysinfo()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&si)?
        .emit()?;

    let client_proto_files = ["./proto/executor_api.proto"];
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .file_descriptor_set_path(out_dir.join("executor_api_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Required for building on Ubuntu 22.04
        .compile_protos(&client_proto_files, &["proto"])?;

    let server_proto_files = [
        "./proto/function_executor.proto",
        "./proto/google/rpc/status.proto",
    ];

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::prost_types")
        .extern_path(".google.rpc", "::tonic_types")
        .file_descriptor_set_path(out_dir.join("function_executor_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Required for building on Ubuntu 22.04
        .compile_protos(&server_proto_files, &["proto"])?;

    Ok(())
}
