use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use vergen::EmitBuilder;


/// Determines if the build directory needs to be rebuilt based on file modification times or existence.
fn needs_rebuild(source_dir: &str, build_dir: &str) -> Result<bool, Box<dyn Error>> {
    let build_path = Path::new(build_dir);
    
    // Check if the build directory does not exist, and return true early if so
    if !build_path.exists() {
        return Ok(true);
    }

    // Get the modification time of the build directory
    let build_time = match build_path.metadata()?.modified() {
        Ok(time) => time,
        Err(_) => return Ok(true), // If we can't get the modification time, assume rebuild is needed
    };

    let node_modules_path = Path::new(source_dir).join("node_modules");

    // Recursively check if any file in the source directory is newer than the build directory, excluding node_modules
    let mut paths_to_check = vec![PathBuf::from(source_dir)];
    while let Some(path) = paths_to_check.pop() {
        if path == node_modules_path {
            continue; // Skip the node_modules directory
        }

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                paths_to_check.push(path);
            } else {
                let modified = entry.metadata()?.modified()?;
                if modified > build_time {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

fn main() -> Result<(), Box<dyn Error>> {
    if needs_rebuild("ui", "ui/build")? {
        println!("Changes detected, building UI...");
        // Execute the npm commands in the 'ui' directory
        if !Command::new("sh")
            .arg("-c")
            .arg("cd ui && npm ci && npm run build")
            .status()?
            .success() {
            return Err("Failed to execute npm commands in the 'ui' directory".into());
        }
    } else {
        println!("UI build is up to date, skipping...");
    }

    // Continue with the rest of the build script
    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .all_git()
        .all_rustc()
        .all_sysinfo()
        .emit()?;

    // Configure and run the tonic build for protobuf files
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
        )?;

    Ok(())
}
