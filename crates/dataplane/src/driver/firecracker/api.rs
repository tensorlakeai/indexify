//! Firecracker REST API client over Unix socket.
//!
//! Implements the subset of the Firecracker API needed for VM lifecycle:
//! boot source, drives, machine config, network interfaces, and instance start.
//! Uses raw HTTP/1.1 over `tokio::net::UnixStream` — no hyper client needed
//! since the API is simple PUT requests, one burst per VM boot.

use std::path::Path;

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

/// Client for the Firecracker API over a Unix socket.
pub struct FirecrackerApiClient {
    socket_path: String,
}

impl FirecrackerApiClient {
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
        }
    }

    /// Wait for the API socket to become available.
    /// Polls every `interval` for up to `timeout`.
    pub async fn wait_for_socket(
        &self,
        timeout: std::time::Duration,
        interval: std::time::Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            if Path::new(&self.socket_path).exists() {
                // Try to connect to verify it's actually listening
                if UnixStream::connect(&self.socket_path).await.is_ok() {
                    return Ok(());
                }
            }
            if start.elapsed() >= timeout {
                bail!(
                    "Timed out waiting for Firecracker API socket at {}",
                    self.socket_path
                );
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// Configure the boot source (kernel image and boot args).
    pub async fn configure_boot_source(
        &self,
        kernel_image_path: &str,
        boot_args: &str,
    ) -> Result<()> {
        let body = serde_json::json!({
            "kernel_image_path": kernel_image_path,
            "boot_args": boot_args,
        });
        self.put("/boot-source", &body).await
    }

    /// Configure the root filesystem drive.
    pub async fn configure_rootfs(&self, drive_path: &str) -> Result<()> {
        let body = serde_json::json!({
            "drive_id": "rootfs",
            "path_on_host": drive_path,
            "is_root_device": true,
            "is_read_only": false,
        });
        self.put("/drives/rootfs", &body).await
    }

    /// Configure the machine (vCPUs and memory).
    pub async fn configure_machine(&self, vcpus: u32, mem_mib: u64) -> Result<()> {
        let body = serde_json::json!({
            "vcpu_count": vcpus,
            "mem_size_mib": mem_mib,
        });
        self.put("/machine-config", &body).await
    }

    /// Configure a network interface with a TAP device and MAC address.
    pub async fn configure_network(&self, tap_dev: &str, mac: &str) -> Result<()> {
        let body = serde_json::json!({
            "iface_id": "eth0",
            "guest_mac": mac,
            "host_dev_name": tap_dev,
        });
        self.put("/network-interfaces/eth0", &body).await
    }

    /// Start the VM instance.
    pub async fn start_instance(&self) -> Result<()> {
        let body = serde_json::json!({
            "action_type": "InstanceStart",
        });
        self.put("/actions", &body).await
    }

    /// Pause the VM (quiesces guest vCPUs and I/O).
    pub async fn pause_vm(&self) -> Result<()> {
        let body = serde_json::json!({
            "state": "Paused",
        });
        self.patch("/vm", &body).await
    }

    /// Configure the balloon device (pre-boot, before InstanceStart).
    ///
    /// `amount_mib` is the balloon SIZE — memory TAKEN from the guest.
    /// So to give the guest 256 MiB out of 512 MiB total, set amount_mib=256.
    pub async fn configure_balloon(&self, amount_mib: u64, deflate_on_oom: bool) -> Result<()> {
        let body = serde_json::json!({
            "amount_mib": amount_mib,
            "deflate_on_oom": deflate_on_oom,
        });
        self.put("/balloon", &body).await
    }

    /// Update the balloon size at runtime (after InstanceStart).
    ///
    /// `amount_mib` is the new balloon SIZE — memory TAKEN from guest.
    /// Decreasing this value (deflation) gives memory back to the guest.
    pub async fn update_balloon(&self, amount_mib: u64) -> Result<()> {
        let body = serde_json::json!({
            "amount_mib": amount_mib,
        });
        self.patch("/balloon", &body).await
    }

    /// Configure a secondary block device (pre-boot, before InstanceStart).
    ///
    /// `cache_type` should be "Unsafe" for warm pool vdb devices.
    pub async fn configure_secondary_drive(
        &self,
        drive_id: &str,
        path: &str,
        is_read_only: bool,
        cache_type: &str,
    ) -> Result<()> {
        let body = serde_json::json!({
            "drive_id": drive_id,
            "path_on_host": path,
            "is_root_device": false,
            "is_read_only": is_read_only,
            "cache_type": cache_type,
        });
        self.put(&format!("/drives/{}", drive_id), &body).await
    }

    /// Send a PATCH request to the Firecracker API.
    async fn patch(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        self.request("PATCH", path, body).await
    }

    /// Send a PUT request to the Firecracker API.
    async fn put(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        self.request("PUT", path, body).await
    }

    /// Send an HTTP request to the Firecracker API.
    async fn request(&self, method: &str, path: &str, body: &serde_json::Value) -> Result<()> {
        let body_str = serde_json::to_string(body)?;
        let request = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            method,
            path,
            body_str.len(),
            body_str
        );

        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to Firecracker socket at {}",
                    self.socket_path
                )
            })?;

        stream.write_all(request.as_bytes()).await?;

        // Read the response before shutting down the write side.
        // Shutting down first can cause Firecracker to close the connection
        // before sending a response.
        let mut response = Vec::with_capacity(4096);
        let mut buf = [0u8; 4096];
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            response.extend_from_slice(&buf[..n]);

            // Search for header/body boundary on raw bytes (avoids per-iteration
            // UTF-8 conversion).
            if let Some(header_end) = find_header_end(&response) {
                let body_start = header_end + 4;
                let content_length = parse_content_length(&response[..header_end]);
                if response.len() >= body_start + content_length {
                    break;
                }
            }
        }

        let response = String::from_utf8_lossy(&response).into_owned();

        // Parse the HTTP status line
        let status_line = response
            .lines()
            .next()
            .context("Empty response from Firecracker API")?;

        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .context("Malformed HTTP status line")?
            .parse()
            .context("Invalid HTTP status code")?;

        if (200..300).contains(&status_code) {
            Ok(())
        } else {
            // Extract body after the blank line
            let body = response.split("\r\n\r\n").nth(1).unwrap_or(&response);
            bail!(
                "Firecracker API {} {} failed with status {}: {}",
                method,
                path,
                status_code,
                body.trim()
            );
        }
    }
}

/// Find the byte offset of `\r\n\r\n` in a byte slice (header/body boundary).
fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Parse Content-Length from raw header bytes.
///
/// Scans lines for a case-insensitive match without allocating.
fn parse_content_length(headers: &[u8]) -> usize {
    let header_str = std::str::from_utf8(headers).unwrap_or("");
    for line in header_str.lines() {
        if line.len() > 15 &&
            line[..15].eq_ignore_ascii_case("content-length:") &&
            let Ok(len) = line[15..].trim().parse::<usize>()
        {
            return len;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use tokio::net::UnixListener;

    use super::*;

    #[tokio::test]
    async fn test_wait_for_socket_success() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("wait-test.sock");
        let sock_str = sock_path.to_string_lossy().to_string();

        // Start listener before waiting
        let _listener = UnixListener::bind(&sock_path).unwrap();

        let client = FirecrackerApiClient::new(&sock_str);
        let result = client
            .wait_for_socket(
                std::time::Duration::from_secs(2),
                std::time::Duration::from_millis(50),
            )
            .await;
        assert!(result.is_ok(), "Should find existing socket");
    }

    #[tokio::test]
    async fn test_wait_for_socket_timeout() {
        let client = FirecrackerApiClient::new("/tmp/nonexistent-socket-12345.sock");
        let result = client
            .wait_for_socket(
                std::time::Duration::from_millis(200),
                std::time::Duration::from_millis(50),
            )
            .await;
        assert!(result.is_err(), "Should timeout for missing socket");
        assert!(
            result.unwrap_err().to_string().contains("Timed out"),
            "Error should mention timeout"
        );
    }
}
