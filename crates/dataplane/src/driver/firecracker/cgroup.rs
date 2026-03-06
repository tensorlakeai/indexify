//! Thin wrapper for runtime cgroup v2 CPU limit updates.
//!
//! The jailer creates the cgroup at `/sys/fs/cgroup/{parent_cgroup}/{vm_id}/`
//! and moves the Firecracker process into it. This module only adjusts
//! `cpu.max` at claim time — no cgroup creation or process attachment.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Handle to a cgroup v2 directory for runtime CPU limit updates.
#[derive(Debug, Clone)]
pub struct CgroupHandle {
    cgroup_path: PathBuf,
}

impl CgroupHandle {
    /// Derive the cgroup path from jailer convention.
    ///
    /// The jailer creates cgroups at:
    /// `/sys/fs/cgroup/{parent_cgroup}/{vm_id}/`
    pub fn from_jailer(parent_cgroup: &str, vm_id: &str) -> Self {
        Self {
            cgroup_path: PathBuf::from("/sys/fs/cgroup")
                .join(parent_cgroup)
                .join(vm_id),
        }
    }

    /// Construct from an explicit path (e.g., recovered from metadata).
    pub fn from_path(path: PathBuf) -> Self {
        Self { cgroup_path: path }
    }

    /// Set a CPU limit in millicores.
    ///
    /// Writes `"{quota} 100000"` to `cpu.max` where
    /// `quota = millicores * 100` (100_000 period).
    pub async fn set_cpu_limit(&self, cpu_millicores: u32) -> Result<()> {
        let quota = cpu_millicores as u64 * 100;
        let value = format!("{} 100000", quota);
        let path = self.cgroup_path.join("cpu.max");
        tokio::fs::write(&path, &value)
            .await
            .with_context(|| format!("Failed to write cpu.max at {}", path.display()))?;
        Ok(())
    }

    /// Remove the CPU limit (set to max).
    pub async fn remove_cpu_limit(&self) -> Result<()> {
        let path = self.cgroup_path.join("cpu.max");
        tokio::fs::write(&path, "max 100000")
            .await
            .with_context(|| format!("Failed to write cpu.max at {}", path.display()))?;
        Ok(())
    }

    /// Get the cgroup path as a string for metadata persistence.
    pub fn path_string(&self) -> String {
        self.cgroup_path.to_string_lossy().into_owned()
    }
}

/// Ensure the parent cgroup exists and has the `cpu` controller delegated.
///
/// The jailer creates child cgroups under `/sys/fs/cgroup/{parent}/`, but
/// requires the parent to exist and have `cpu` in `cgroup.subtree_control`.
pub fn ensure_parent_cgroup(parent_cgroup: &str) -> Result<()> {
    let parent = std::path::Path::new("/sys/fs/cgroup").join(parent_cgroup);
    std::fs::create_dir_all(&parent)
        .with_context(|| format!("Failed to create parent cgroup {}", parent.display()))?;

    let subtree_control = parent.join("cgroup.subtree_control");
    let current = std::fs::read_to_string(&subtree_control).unwrap_or_default();
    let has_cpu = current.split_whitespace().any(|c| c == "cpu");
    if !has_cpu {
        std::fs::write(&subtree_control, "+cpu").with_context(|| {
            format!(
                "Failed to enable cpu controller in {}",
                subtree_control.display()
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_jailer_path() {
        let handle = CgroupHandle::from_jailer("indexify", "vm-abc123");
        assert_eq!(handle.path_string(), "/sys/fs/cgroup/indexify/vm-abc123");
    }

    #[test]
    fn test_from_explicit_path() {
        let handle = CgroupHandle::from_path(PathBuf::from("/sys/fs/cgroup/custom/path"));
        assert_eq!(handle.path_string(), "/sys/fs/cgroup/custom/path");
    }
}
