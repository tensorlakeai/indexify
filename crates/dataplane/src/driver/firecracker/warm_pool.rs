//! Warm VM pool manager for fast container startup.
//!
//! Maintains a pool of pre-booted Firecracker VMs that can be claimed
//! in ~130ms instead of cold-booting (~1.4s). VMs are fully destroyed
//! on release — no recycling complexity.

use std::collections::VecDeque;

use tokio::sync::mpsc;
use tracing::{info, warn};

use super::cgroup::CgroupHandle;
use crate::config::WarmPoolConfig;

/// A pre-booted VM ready to be claimed.
pub struct WarmVm {
    /// Handle ID (e.g., "fc-abc123").
    pub handle_id: String,
    /// Unique VM identifier.
    pub vm_id: String,
    /// Path to Firecracker API socket.
    pub socket_path: String,
    /// Daemon gRPC address (ip:port).
    pub daemon_addr: String,
    /// Daemon HTTP address (ip:port).
    pub http_addr: String,
    /// Guest IP address.
    pub guest_ip: String,
    /// Thin LV name for the VM's rootfs.
    pub lv_name: String,
    /// Network namespace name.
    pub netns_name: String,
    /// Device-mapper device name for vdb (e.g., "dm-vdb-abc123").
    pub dm_device_name: String,
    /// Empty thin LV name used as initial vdb backing.
    pub empty_lv_name: String,
    /// Cgroup handle for runtime CPU adjustments.
    pub cgroup: CgroupHandle,
    /// Maximum memory in MiB the VM was booted with.
    pub max_memory_mib: u64,
    /// Maximum vCPUs the VM was booted with.
    pub max_vcpus: u32,
    /// PID of the Firecracker process.
    pub pid: u32,
}

/// Warm VM pool state.
pub struct WarmPool {
    /// Available warm VMs (FIFO queue).
    pool: VecDeque<WarmVm>,
    /// Configuration.
    config: WarmPoolConfig,
    /// Total VMs managed (warm + claimed).
    total_vms: usize,
    /// Number of VMs currently booting.
    inflight_boots: usize,
    /// Consecutive boot failures for backoff.
    consecutive_failures: usize,
    /// Channel to signal replenishment.
    replenish_tx: mpsc::Sender<()>,
}

/// Maximum consecutive failures before pausing replenishment.
const MAX_CONSECUTIVE_FAILURES: usize = 5;

impl WarmPool {
    /// Create a new warm pool with the given config and replenishment channel.
    pub fn new(config: WarmPoolConfig, replenish_tx: mpsc::Sender<()>) -> Self {
        Self {
            pool: VecDeque::with_capacity(config.target_count),
            config,
            total_vms: 0,
            inflight_boots: 0,
            consecutive_failures: 0,
            replenish_tx,
        }
    }

    /// Try to claim a warm VM from the pool.
    ///
    /// Returns `None` if the pool is empty (caller should fall back to cold
    /// boot).
    pub fn try_claim(&mut self) -> Option<WarmVm> {
        let vm = self.pool.pop_front()?;
        info!(
            vm_id = %vm.vm_id,
            pool_remaining = self.pool.len(),
            "Claimed warm VM"
        );
        // Signal replenishment (non-blocking — ok if channel is full)
        let _ = self.replenish_tx.try_send(());
        Some(vm)
    }

    /// Notify that a claimed VM has been destroyed.
    ///
    /// Decrements total count and signals replenishment.
    pub fn notify_destroyed(&mut self) {
        self.total_vms = self.total_vms.saturating_sub(1);
        let _ = self.replenish_tx.try_send(());
    }

    /// Push a newly booted warm VM into the pool.
    pub fn push_warm(&mut self, vm: WarmVm) {
        info!(
            vm_id = %vm.vm_id,
            pool_size = self.pool.len() + 1,
            target = self.config.target_count,
            "Warm VM added to pool"
        );
        self.pool.push_back(vm);
        self.total_vms += 1;
        self.consecutive_failures = 0;
    }

    /// Record that an inflight boot completed (success or failure).
    pub fn boot_completed(&mut self) {
        self.inflight_boots = self.inflight_boots.saturating_sub(1);
    }

    /// Record a boot failure.
    pub fn record_boot_failure(&mut self) {
        self.consecutive_failures += 1;
        self.inflight_boots = self.inflight_boots.saturating_sub(1);
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            warn!(
                failures = self.consecutive_failures,
                "Warm pool: too many consecutive boot failures, pausing replenishment"
            );
        }
    }

    /// Check if replenishment is needed and return the number of VMs to boot.
    pub fn replenish_count(&self) -> usize {
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            return 0;
        }
        if self.total_vms + self.inflight_boots >= self.config.max_total_vms {
            return 0;
        }
        let current = self.pool.len() + self.inflight_boots;
        if current >= self.config.target_count {
            return 0;
        }
        let needed = self.config.target_count - current;
        // Cap by max_total_vms headroom
        let headroom = self
            .config
            .max_total_vms
            .saturating_sub(self.total_vms + self.inflight_boots);
        needed.min(headroom).min(self.config.max_parallel_boots)
    }

    /// Increment inflight boot counter by `n`.
    pub fn start_boots(&mut self, n: usize) {
        self.inflight_boots += n;
    }

    /// Reset failure counter (e.g., after periodic retry timer).
    pub fn reset_failures(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Memory overhead in bytes for resource reporting.
    ///
    /// Uses `target_count * idle_memory_mib` (steady-state overhead).
    pub fn overhead_memory_bytes(&self) -> u64 {
        self.config.overhead_memory_bytes()
    }

    /// Current number of warm VMs available.
    pub fn available(&self) -> usize {
        self.pool.len()
    }

    /// Configuration reference.
    pub fn config(&self) -> &WarmPoolConfig {
        &self.config
    }

    /// Create the replenishment signal receiver.
    pub fn create_replenish_channel() -> (mpsc::Sender<()>, mpsc::Receiver<()>) {
        mpsc::channel(16)
    }

    /// Total VMs (warm + claimed).
    pub fn total_vms(&self) -> usize {
        self.total_vms
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> WarmPoolConfig {
        WarmPoolConfig {
            target_count: 3,
            max_total_vms: 10,
            max_parallel_boots: 2,
            idle_cpu_millicores: 50,
            idle_memory_mib: 32,
            max_vcpus: None,
            max_memory_mib: None,
            parent_cgroup: "indexify".to_string(),
        }
    }

    fn test_warm_vm(id: &str) -> WarmVm {
        WarmVm {
            handle_id: format!("fc-{}", id),
            vm_id: id.to_string(),
            socket_path: format!("/tmp/fc-{}.sock", id),
            daemon_addr: "192.168.30.2:9500".to_string(),
            http_addr: "192.168.30.2:9501".to_string(),
            guest_ip: "192.168.30.2".to_string(),
            lv_name: format!("indexify-vm-{}", id),
            netns_name: format!("indexify-vm-{}", id),
            dm_device_name: format!("dm-vdb-{}", id),
            empty_lv_name: format!("indexify-empty-{}", id),
            cgroup: CgroupHandle::from_jailer("indexify", id),
            max_memory_mib: 512,
            max_vcpus: 2,
            pid: 12345,
        }
    }

    #[test]
    fn test_replenish_count_empty_pool() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let pool = WarmPool::new(test_config(), tx);
        // Pool is empty, target=3, max_parallel=2 → should boot 2
        assert_eq!(pool.replenish_count(), 2);
    }

    #[test]
    fn test_replenish_count_partial_pool() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        pool.push_warm(test_warm_vm("1"));
        pool.push_warm(test_warm_vm("2"));
        // 2 in pool, target=3 → need 1
        assert_eq!(pool.replenish_count(), 1);
    }

    #[test]
    fn test_replenish_count_full_pool() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        pool.push_warm(test_warm_vm("1"));
        pool.push_warm(test_warm_vm("2"));
        pool.push_warm(test_warm_vm("3"));
        assert_eq!(pool.replenish_count(), 0);
    }

    #[test]
    fn test_claim_and_replenish_signal() {
        let (tx, mut rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        pool.push_warm(test_warm_vm("1"));

        let claimed = pool.try_claim();
        assert!(claimed.is_some());
        assert_eq!(claimed.unwrap().vm_id, "1");
        assert_eq!(pool.available(), 0);

        // Should have sent a replenish signal
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_claim_empty_pool() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        assert!(pool.try_claim().is_none());
    }

    #[test]
    fn test_failure_backoff() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            pool.record_boot_failure();
        }
        assert_eq!(pool.replenish_count(), 0);

        // Reset should allow replenishment again
        pool.reset_failures();
        assert!(pool.replenish_count() > 0);
    }

    #[test]
    fn test_max_total_vms_cap() {
        let mut config = test_config();
        config.max_total_vms = 4;
        config.target_count = 3;
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(config, tx);

        // Simulate 3 claimed VMs
        pool.total_vms = 3;
        // Only 1 more allowed (max=4, total=3)
        assert_eq!(pool.replenish_count(), 1);
    }

    #[test]
    fn test_overhead_memory_bytes() {
        let (tx, _rx) = WarmPool::create_replenish_channel();
        let pool = WarmPool::new(test_config(), tx);
        // 3 targets * 32 MiB * 1024 * 1024
        assert_eq!(pool.overhead_memory_bytes(), 3 * 32 * 1024 * 1024);
    }

    #[test]
    fn test_notify_destroyed() {
        let (tx, mut rx) = WarmPool::create_replenish_channel();
        let mut pool = WarmPool::new(test_config(), tx);
        pool.push_warm(test_warm_vm("1"));
        let _ = rx.try_recv(); // drain push signal

        pool.notify_destroyed();
        assert_eq!(pool.total_vms(), 0);
        // Should have sent replenish signal
        assert!(rx.try_recv().is_ok());
    }
}
