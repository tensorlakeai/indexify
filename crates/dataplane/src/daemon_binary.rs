//! Embedded daemon binary extraction.
//!
//! The container daemon binary is embedded in the dataplane binary at compile
//! time and extracted at runtime to a temporary location. This extracted binary
//! is then bind-mounted into containers.
//!
//! When `RUN_DOCKER_TESTS=1` is set during build on macOS, the binary is
//! cross-compiled for Linux using Docker, enabling Docker integration tests.

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use anyhow::{Context, Result};
use tracing::info;

/// The embedded daemon binary (compiled at build time).
const DAEMON_BINARY: &[u8] = include_bytes!(env!("DAEMON_BINARY_PATH"));

/// The target triple the daemon binary was built for.
#[allow(dead_code)] // Used by integration tests
pub const DAEMON_BINARY_TARGET: &str = env!("DAEMON_BINARY_TARGET");

/// Path where the daemon binary is extracted.
static DAEMON_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Default extraction path for the daemon binary.
const DEFAULT_DAEMON_PATH: &str = "/tmp/indexify-container-daemon";

/// Extract the embedded daemon binary to the filesystem.
/// This should be called once at startup.
pub fn extract_daemon_binary() -> Result<&'static Path> {
    let path = DAEMON_PATH.get_or_init(|| {
        let path = PathBuf::from(DEFAULT_DAEMON_PATH);

        // Extract the binary
        if let Err(e) = extract_to_path(&path) {
            tracing::error!(error = %e, "Failed to extract daemon binary");
            // Return a path even on failure - the actual error will be caught
            // when trying to use it
        }

        path
    });

    // Verify the file exists and is executable
    if !path.exists() {
        anyhow::bail!("Daemon binary not found at {}", path.display());
    }

    Ok(path.as_path())
}

/// Extract the daemon binary to the specified path.
fn extract_to_path(path: &Path) -> Result<()> {
    info!(path = %path.display(), size = DAEMON_BINARY.len(), "Extracting daemon binary");

    // Write the binary
    std::fs::write(path, DAEMON_BINARY).context("Failed to write daemon binary")?;

    // Make it executable (rwxr-xr-x = 0o755)
    let mut perms = std::fs::metadata(path)
        .context("Failed to get daemon binary metadata")?
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).context("Failed to set daemon binary permissions")?;

    info!(path = %path.display(), "Daemon binary extracted successfully");
    Ok(())
}

/// Get the path to the extracted daemon binary.
/// Returns an error if the binary hasn't been extracted yet.
pub fn get_daemon_path() -> Result<&'static Path> {
    DAEMON_PATH
        .get()
        .map(|p| p.as_path())
        .context("Daemon binary not extracted yet - call extract_daemon_binary() first")
}

/// Check if the daemon binary has been extracted.
#[allow(dead_code)] // Public API for potential external use
pub fn is_extracted() -> bool {
    DAEMON_PATH.get().is_some()
}

/// Check if the daemon binary can run in Linux Docker containers.
/// Returns true if the binary was built for Linux.
#[allow(dead_code)] // Used by integration tests
pub fn is_linux_binary() -> bool {
    DAEMON_BINARY_TARGET.contains("linux")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::const_is_empty)] // The assertion is intentional for documentation
    fn test_daemon_binary_is_embedded() {
        // The binary should be non-empty
        assert!(!DAEMON_BINARY.is_empty());

        // Should start with ELF magic number on Linux or Mach-O on macOS
        #[cfg(target_os = "linux")]
        assert_eq!(&DAEMON_BINARY[0..4], b"\x7fELF");

        #[cfg(target_os = "macos")]
        {
            // Mach-O magic numbers (both 32 and 64 bit, both endianness)
            let magic = u32::from_ne_bytes([
                DAEMON_BINARY[0],
                DAEMON_BINARY[1],
                DAEMON_BINARY[2],
                DAEMON_BINARY[3],
            ]);
            assert!(
                magic == 0xfeedface ||
                    magic == 0xfeedfacf ||
                    magic == 0xcefaedfe ||
                    magic == 0xcffaedfe
            );
        }
    }
}
