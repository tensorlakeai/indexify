use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = ["./proto/executor_api.proto"];
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(false) // Don't build client code as it's not needed for now
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("executor_api_descriptor.bin"))
        .compile_protos(&proto_files, &["proto"])?;
    Ok(())
}
