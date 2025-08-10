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

    let proto_files = ["../server/proto/executor_api.proto"];
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .build_client(false) // Don't build client code as it's not needed for now
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("executor_api_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Required for building on Ubuntu 22.04
        .compile_protos(&proto_files, &["../server/proto"])?;

    Ok(())
}
