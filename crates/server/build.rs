use std::{env, process::Command};

use anyhow::{Result, anyhow};
use vergen::{BuildBuilder, Emitter, SysinfoBuilder};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=ui/src");
    println!("cargo:rerun-if-changed=ui/tsconfig.json");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/package-lock.json");
    println!("cargo:rerun-if-changed=ui/public");

    if !Command::new("npm")
        .arg("ci")
        .current_dir("ui")
        .status()?
        .success()
    {
        return Err(anyhow!("Failed to execute `npm ci` in the 'ui' directory"));
    }

    let node_options = node_options_with_global_webcrypto();
    if !Command::new("npm")
        .arg("run")
        .arg("build")
        .current_dir("ui")
        .env("NODE_OPTIONS", node_options)
        .status()?
        .success()
    {
        return Err(anyhow!(
            "Failed to execute `npm run build` in the 'ui' directory"
        ));
    }

    let build = BuildBuilder::all_build()?;
    let si = SysinfoBuilder::all_sysinfo()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&si)?
        .emit()?;

    Ok(())
}

fn node_options_with_global_webcrypto() -> String {
    const FLAG: &str = "--experimental-global-webcrypto";
    match env::var("NODE_OPTIONS") {
        Ok(existing) => {
            if existing.split_whitespace().any(|opt| opt == FLAG) {
                existing
            } else if existing.trim().is_empty() {
                FLAG.to_string()
            } else {
                format!("{existing} {FLAG}")
            }
        }
        Err(_) => FLAG.to_string(),
    }
}
