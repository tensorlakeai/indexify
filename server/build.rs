use std::{env, path::PathBuf, process::Command};

use anyhow::{anyhow, Result};
use vergen::{BuildBuilder, Emitter, SysinfoBuilder};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=ui/src");
    println!("cargo:rerun-if-changed=ui/tsconfig.json");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/package-lock.json");
    println!("cargo:rerun-if-changed=ui/public");

    if !Command::new("sh")
        .arg("-c")
        .arg("cd ui && npm ci && npm run build")
        .status()?
        .success()
    {
        return Err(anyhow!(
            "Failed to execute npm commands in the 'ui' directory"
        ));
    }

    let build = BuildBuilder::all_build()?;
    let si = SysinfoBuilder::all_sysinfo()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&si)?
        .emit()?;

    let proto_files = ["./proto/executor_api_v3.proto"];
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .build_client(false) // Don't build client code as it's not needed for now
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("executor_api_v3_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Required for building on Ubuntu 22.04
        .compile_protos(&proto_files, &["proto"])?;

    Ok(())
}
