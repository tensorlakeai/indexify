use std::{env, path::PathBuf, process::Command};

use anyhow::anyhow;
use tracing::info;

pub fn set_python_path(path: &str) -> Result<(), anyhow::Error> {
    let path = PathBuf::from(path);
    let parent_path = path
        .parent()
        .ok_or(anyhow::anyhow!(
            "error setting PYTHONPATH: unable to get parent path of {:?}",
            path
        ))?
        .to_str()
        .ok_or(anyhow!(
            "error setting PYTHONPATH: unable to get str represtation of parent path of {:?}",
            path
        ))?;
    let python_path = std::env::var("PYTHONPATH").unwrap_or("".to_string());
    let mut site_packages: String = "".into();
    // THIS IS NEEEDED FOR MAC OS.
    if env::var("VIRTUAL_ENV").is_ok() {
        // Use Python itself to get the site-packages path
        let output = Command::new("python")
            .arg("-c")
            .arg("import site; print(site.getsitepackages()[0])")
            .output()
            .expect("Failed to execute Python");

        let site_packages_path = if output.status.success() {
            let path_str = String::from_utf8_lossy(&output.stdout);
            let path_trimmed = path_str.trim();
            Some(PathBuf::from(path_trimmed))
        } else {
            None
        };
        if let Some(site_packages_path) = site_packages_path {
            site_packages = site_packages_path
                .to_str()
                .ok_or(anyhow!("error setting PYTHONPATH: invalid path"))?
                .into();
        }
    }
    let new_python_path = format!("{}:{}:{}", python_path, parent_path, site_packages);
    info!("PYTHONPATH set to: {}", &new_python_path);
    env::set_var("PYTHONPATH", new_python_path);
    Ok(())
}
