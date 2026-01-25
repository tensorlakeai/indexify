//! Network policy enforcement via iptables DOCKER-USER chain.
//!
//! Creates per-container chains: INDEXIFY-SB-<container_id>
//! Uses RETURN for allow, DROP for block.
//!
//! Rule structure per sandbox:
//! 1. Jump from DOCKER-USER → INDEXIFY-SB-<container_id> (for this container's
//!    IP)
//! 2. RETURN rules for allowed destinations
//! 3. DROP rules for denied destinations
//! 4. Default DROP (if allow_internet_access=false) or RETURN (if true)

use std::{process::Command, sync::Mutex};

use anyhow::{Context, Result};
use proto_api::executor_api_pb::NetworkPolicy;
use tracing::{info, warn};

lazy_static::lazy_static! {
    /// Global lock to serialize iptables operations.
    /// iptables commands are not atomic and can fail if multiple
    /// processes try to modify the rules concurrently.
    static ref IPTABLES_LOCK: Mutex<()> = Mutex::new(());
}

/// Generate the chain name for a container.
/// Uses first 12 characters of container ID for readability.
fn chain_name(container_id: &str) -> String {
    let id_prefix = if container_id.len() > 12 {
        &container_id[..12]
    } else {
        container_id
    };
    format!("INDEXIFY-SB-{}", id_prefix)
}

/// Apply network rules for a container.
///
/// This should be called immediately after the container starts and has an IP,
/// but before the daemon starts executing user code.
///
/// # Arguments
/// * `container_id` - The Docker container ID
/// * `container_ip` - The container's IP address on the Docker network
/// * `policy` - The network policy to apply
pub fn apply_rules(container_id: &str, container_ip: &str, policy: &NetworkPolicy) -> Result<()> {
    let _lock = IPTABLES_LOCK.lock().unwrap();
    let chain = chain_name(container_id);

    info!(
        container_id = container_id,
        container_ip = container_ip,
        chain = chain,
        allow_internet = policy.allow_internet_access.unwrap_or(true),
        allow_count = policy.allow_out.len(),
        deny_count = policy.deny_out.len(),
        "Applying network policy"
    );

    // Create chain (ignore error if exists)
    let _ = iptables(&["-N", &chain]);

    // Flush existing rules in case of re-application
    iptables(&["-F", &chain])?;

    // Add jump from DOCKER-USER to our chain (for this container's source IP)
    // First try to delete in case it exists, then add
    let _ = iptables(&["-D", "DOCKER-USER", "-s", container_ip, "-j", &chain]);
    iptables(&["-I", "DOCKER-USER", "-s", container_ip, "-j", &chain])?;

    // Allow established connections (stateful rule)
    // This ensures that responses to outbound connections are allowed
    iptables(&[
        "-A",
        &chain,
        "-m",
        "state",
        "--state",
        "ESTABLISHED,RELATED",
        "-j",
        "RETURN",
    ])?;

    // Process allow rules (RETURN = allow traffic to continue to other rules)
    // Allow rules take precedence, so we add them first
    for dest in &policy.allow_out {
        info!(
            container_id = container_id,
            dest = dest,
            "Adding ALLOW rule"
        );
        iptables(&["-A", &chain, "-d", dest, "-j", "RETURN"])?;
    }

    // Process deny rules (DROP)
    for dest in &policy.deny_out {
        info!(container_id = container_id, dest = dest, "Adding DENY rule");
        iptables(&["-A", &chain, "-d", dest, "-j", "DROP"])?;
    }

    // Default action based on allow_internet_access setting
    let allow_internet = policy.allow_internet_access.unwrap_or(true);
    if !allow_internet {
        // Block all internet by default
        iptables(&["-A", &chain, "-j", "DROP"])?;
        info!(
            container_id = container_id,
            "Network policy: default DROP (internet blocked)"
        );
    } else {
        // Allow all internet by default (just return to continue processing)
        iptables(&["-A", &chain, "-j", "RETURN"])?;
        info!(
            container_id = container_id,
            "Network policy: default ALLOW (internet allowed)"
        );
    }

    Ok(())
}

/// Remove network rules for a container.
///
/// This should be called when the container is being terminated.
/// It cleans up the iptables chain and removes the jump rule.
///
/// # Arguments
/// * `container_id` - The Docker container ID
/// * `container_ip` - The container's IP address (used to remove the jump rule)
pub fn remove_rules(container_id: &str, container_ip: &str) -> Result<()> {
    let _lock = IPTABLES_LOCK.lock().unwrap();
    let chain = chain_name(container_id);

    info!(
        container_id = container_id,
        container_ip = container_ip,
        chain = chain,
        "Removing network policy rules"
    );

    // Remove jump rule from DOCKER-USER
    // Ignore errors - rule might not exist
    if let Err(e) = iptables(&["-D", "DOCKER-USER", "-s", container_ip, "-j", &chain]) {
        warn!(
            container_id = container_id,
            error = %e,
            "Failed to remove jump rule (may not exist)"
        );
    }

    // Flush the chain
    if let Err(e) = iptables(&["-F", &chain]) {
        warn!(
            container_id = container_id,
            error = %e,
            "Failed to flush chain (may not exist)"
        );
    }

    // Delete the chain
    if let Err(e) = iptables(&["-X", &chain]) {
        warn!(
            container_id = container_id,
            error = %e,
            "Failed to delete chain (may not exist)"
        );
    }

    Ok(())
}

/// Execute an iptables command.
fn iptables(args: &[&str]) -> Result<()> {
    let output = Command::new("iptables")
        .args(args)
        .output()
        .context("Failed to execute iptables command")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("iptables {} failed: {}", args.join(" "), stderr.trim());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_name() {
        assert_eq!(chain_name("abcdef123456789"), "INDEXIFY-SB-abcdef123456");
        assert_eq!(chain_name("short"), "INDEXIFY-SB-short");
        assert_eq!(chain_name("exactly12ch"), "INDEXIFY-SB-exactly12ch");
    }

    #[test]
    fn test_chain_name_truncation() {
        // Container IDs are typically 64 characters
        let long_id = "a".repeat(64);
        let chain = chain_name(&long_id);
        assert_eq!(chain, "INDEXIFY-SB-aaaaaaaaaaaa");
        assert_eq!(chain.len(), 24); // "INDEXIFY-SB-" (12) + 12 chars
    }
}
