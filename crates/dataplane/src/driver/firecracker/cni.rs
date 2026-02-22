//! CNI (Container Network Interface) networking for Firecracker VMs.
//!
//! Invokes `cnitool` to manage per-VM network namespaces and TAP devices.
//! The operator configures a CNI conflist at `/etc/cni/net.d/<name>.conflist`
//! with plugins like `bridge` + `firewall` + `tc-redirect-tap`.

use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

/// Result of CNI network setup for a VM.
#[derive(Debug, Clone)]
pub struct CniResult {
    /// IP address allocated to the guest.
    pub guest_ip: String,
    /// Name of the TAP device on the host for Firecracker.
    pub tap_device: String,
    /// MAC address for the guest network interface.
    pub guest_mac: String,
    /// Name of the network namespace (e.g., "indexify-vm-abc123").
    pub netns_name: String,
}

/// Manages CNI networking for Firecracker VMs.
pub struct CniManager {
    /// CNI network name (matches conflist "name" field).
    network_name: String,
    /// Path to CNI plugin binaries.
    cni_bin_path: String,
}

impl CniManager {
    pub fn new(network_name: String, cni_bin_path: String) -> Self {
        Self {
            network_name,
            cni_bin_path,
        }
    }

    /// Get the CNI network name.
    pub fn network_name(&self) -> &str {
        &self.network_name
    }

    /// Get the CNI bin path.
    pub fn cni_bin_path(&self) -> &str {
        &self.cni_bin_path
    }

    /// Set up networking for a VM: create netns, invoke CNI, parse results.
    pub async fn setup_network(&self, vm_id: &str) -> Result<CniResult> {
        let netns_name = format!("indexify-vm-{}", vm_id);
        let netns_path = format!("/var/run/netns/{}", netns_name);

        // 1. Create network namespace (clean up stale one first if present)
        if std::path::Path::new(&netns_path).exists() {
            // Run cnitool del first to release the IP allocation
            let _ = Command::new("cnitool")
                .args(["del", &self.network_name, &netns_path])
                .env("CNI_PATH", &self.cni_bin_path)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .output()
                .await;
            let _ = self.delete_netns(&netns_name).await;
        }

        let output = Command::new("ip")
            .args(["netns", "add", &netns_name])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to execute 'ip netns add'")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to create network namespace {}: {}", netns_name, stderr);
        }

        // 2. Invoke cnitool to add the network
        let output = Command::new("cnitool")
            .args(["add", &self.network_name, &netns_path])
            .env("CNI_PATH", &self.cni_bin_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to execute 'cnitool add'")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Clean up netns on failure
            let _ = self.delete_netns(&netns_name).await;
            bail!(
                "cnitool add failed for network {} in {}: {}",
                self.network_name,
                netns_name,
                stderr
            );
        }

        // 3. Parse the JSON result from cnitool
        let stdout = String::from_utf8(output.stdout)
            .context("cnitool output is not valid UTF-8")?;
        let cni_output: serde_json::Value =
            serde_json::from_str(&stdout).context("Failed to parse cnitool JSON output")?;

        // Extract the IP address from the CNI result
        let guest_ip = cni_output["ips"]
            .as_array()
            .and_then(|ips| ips.first())
            .and_then(|ip| ip["address"].as_str())
            .context("No IP address in cnitool output")?;

        // Strip CIDR prefix if present (e.g., "192.168.30.2/24" -> "192.168.30.2")
        let guest_ip = guest_ip
            .split('/')
            .next()
            .unwrap_or(guest_ip)
            .to_string();

        // 4. Determine TAP device name
        // tc-redirect-tap creates a TAP device named "tap{index}" in the
        // network namespace. We look for it by listing interfaces.
        let tap_device = self
            .find_tap_device(&netns_name)
            .await
            .unwrap_or_else(|_| format!("tap0"));

        // 5. Generate deterministic MAC address from vm_id
        let guest_mac = generate_mac(vm_id);

        Ok(CniResult {
            guest_ip,
            tap_device,
            guest_mac,
            netns_name,
        })
    }

    /// Tear down networking for a VM (idempotent).
    pub async fn teardown_network(&self, vm_id: &str) {
        let netns_name = format!("indexify-vm-{}", vm_id);
        let netns_path = format!("/var/run/netns/{}", netns_name);

        // 1. Invoke cnitool del (tolerate errors)
        let _ = Command::new("cnitool")
            .args(["del", &self.network_name, &netns_path])
            .env("CNI_PATH", &self.cni_bin_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await;

        // 2. Delete network namespace (tolerate errors)
        let _ = self.delete_netns(&netns_name).await;
    }

    /// List all network namespaces with the indexify-vm- prefix.
    pub async fn list_netns(&self) -> Result<Vec<String>> {
        let output = Command::new("ip")
            .args(["netns", "list"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to execute 'ip netns list'")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let namespaces: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                // ip netns list output: "name (id: N)" or just "name"
                let name = line.split_whitespace().next()?;
                if name.starts_with("indexify-vm-") {
                    Some(name.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(namespaces)
    }

    /// Find the TAP device created by tc-redirect-tap in the network namespace.
    async fn find_tap_device(&self, netns_name: &str) -> Result<String> {
        let output = Command::new("ip")
            .args(["netns", "exec", netns_name, "ip", "-j", "link", "show"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to list interfaces in netns")?;

        let stdout = String::from_utf8(output.stdout)?;
        let interfaces: Vec<serde_json::Value> = serde_json::from_str(&stdout)
            .unwrap_or_default();

        // Look for a TAP device (name starts with "tap")
        for iface in &interfaces {
            if let Some(name) = iface["ifname"].as_str() {
                if name.starts_with("tap") {
                    return Ok(name.to_string());
                }
            }
        }

        bail!("No TAP device found in namespace {}", netns_name)
    }

    /// Delete a network namespace.
    async fn delete_netns(&self, netns_name: &str) -> Result<()> {
        let output = Command::new("ip")
            .args(["netns", "del", netns_name])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("Failed to execute 'ip netns del'")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to delete netns {}: {}", netns_name, stderr);
        }
        Ok(())
    }
}

/// Generate a deterministic MAC address from a VM ID.
///
/// Uses the first 5 bytes of the SHA-256 hash of the VM ID, with the
/// locally administered bit set (02:xx:xx:xx:xx:xx).
pub(crate) fn generate_mac(vm_id: &str) -> String {
    use sha2::{Digest, Sha256};

    let hash = Sha256::digest(vm_id.as_bytes());
    format!(
        "02:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        hash[0], hash[1], hash[2], hash[3], hash[4]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_mac_format() {
        let mac = generate_mac("test-vm-1");
        // Must start with 02: (locally administered)
        assert!(mac.starts_with("02:"), "MAC should be locally administered");
        // Must have 6 colon-separated octets
        assert_eq!(mac.split(':').count(), 6, "MAC should have 6 octets");
        // Each octet must be valid hex
        for octet in mac.split(':') {
            assert_eq!(octet.len(), 2, "Each octet should be 2 hex chars");
            assert!(
                u8::from_str_radix(octet, 16).is_ok(),
                "Each octet should be valid hex"
            );
        }
    }

    #[test]
    fn test_generate_mac_deterministic() {
        let mac1 = generate_mac("same-id");
        let mac2 = generate_mac("same-id");
        assert_eq!(mac1, mac2, "Same VM ID should produce same MAC");
    }

    #[test]
    fn test_generate_mac_unique_per_id() {
        let mac1 = generate_mac("vm-1");
        let mac2 = generate_mac("vm-2");
        assert_ne!(mac1, mac2, "Different VM IDs should produce different MACs");
    }

    #[test]
    fn test_cni_manager_accessors() {
        let cni = CniManager::new("test-network".to_string(), "/opt/cni/bin".to_string());
        assert_eq!(cni.network_name(), "test-network");
        assert_eq!(cni.cni_bin_path(), "/opt/cni/bin");
    }
}
