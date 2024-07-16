use std::process::Command;

use anyhow::{anyhow, Result};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=../../ui/src");
    println!("cargo:rerun-if-changed=../../ui/tsconfig.json");
    println!("cargo:rerun-if-changed=../../ui/package.json");
    println!("cargo:rerun-if-changed=../../ui/package-lock.json");
    println!("cargo:rerun-if-changed=../../ui/public");

    if !Command::new("sh")
        .arg("-c")
        .arg("cd ../../ui && npm i && npm run build")
        .status()?
        .success()
    {
        return Err(anyhow!(
            "Failed to execute npm commands in the 'ui' directory"
        ));
    }

    Ok(())
}
