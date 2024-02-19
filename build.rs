use std::error::Error;
use std::process::Command;
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    // Execute the npm commands in the 'ui' directory
    if !Command::new("sh")
        .arg("-c")
        .arg("cd ui && npm ci && npm run build")
        .status()?
        .success() {
        return Err("Failed to execute npm commands in the 'ui' directory".into());
    }

    // Continue with the rest of the build script
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .all_git()
        .all_rustc()
        .all_sysinfo()
        .emit()?;

    tonic_build::configure()
        .out_dir("crates/indexify_proto/src/")
        .type_attribute(
            "CreateContentRequest",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .type_attribute(
            "ContentMetadata",
            "#[derive(serde::Deserialize, serde::Serialize)]",
        )
        .compile(
            &["proto/coordinator_service.proto", "proto/raft.proto"],
            &["proto"],
        )
        .unwrap();

    Ok(())
}
