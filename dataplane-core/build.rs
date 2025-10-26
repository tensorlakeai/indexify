use std::{env, path::PathBuf};

use anyhow::Result;

fn main() -> Result<()> {
    let proto_files = ["../server/proto/executor_api.proto"];
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .build_client(true) // Build client code for dataplane
        .build_server(false) // Don't build server code
        .file_descriptor_set_path(out_dir.join("executor_api_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Required for building on Ubuntu 22.04
        .compile_protos(&proto_files, &["../server/proto"])?;

    Ok(())
}
