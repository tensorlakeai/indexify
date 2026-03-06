//! Firecracker microVM driver for Indexify dataplane.
//!
//! Provides hardware-virtualized isolation using Firecracker microVMs with
//! dm-thin native snapshots for rootfs and CNI networking.
//!
//! Uses LVM thin provisioning's native snapshot capability. The base rootfs
//! is imported into a thin LV, and each VM gets a thin snapshot that can be
//! independently resized for per-VM disk sizing with COW block sharing.

pub(crate) mod api;
pub(crate) mod cgroup;
mod cni;
pub(crate) mod dm_swap;
pub(crate) mod dm_thin;
mod log_stream;
mod rootfs;
pub(crate) mod vm_state;
pub(crate) mod warm_pool;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use self::{
    api::FirecrackerApiClient,
    cni::CniManager,
    dm_thin::BaseImageHandle,
    vm_state::{
        BaseImageMetadata,
        VmMetadata,
        VmProcess,
        VmState,
        is_firecracker_process,
        scan_metadata_files,
    },
};
use super::{
    DAEMON_GRPC_PORT,
    DAEMON_HTTP_PORT,
    ExitStatus,
    ProcessConfig,
    ProcessDriver,
    ProcessHandle,
};

/// Default rootfs size in bytes (1 GiB).
const DEFAULT_ROOTFS_SIZE_BYTES: u64 = 1024 * 1024 * 1024;
/// Default vCPUs per VM.
const DEFAULT_VCPU_COUNT: u32 = 2;
/// Default memory per VM in MiB.
const DEFAULT_MEMORY_MIB: u64 = 512;
/// Default CNI bin path.
const DEFAULT_CNI_BIN_PATH: &str = "/opt/cni/bin";
/// Default guest netmask.
const DEFAULT_GUEST_NETMASK: &str = "255.255.255.0";
/// Timeout for waiting for Firecracker API socket.
const API_SOCKET_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval for API socket.
const API_SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Resolve a binary name to an absolute path.
///
/// The jailer's `--exec-file` requires an absolute path. If the caller
/// provides a bare name (e.g., "firecracker"), resolve it via PATH.
fn resolve_binary_path(name: &str) -> Result<String> {
    let path = PathBuf::from(name);
    if path.is_absolute() {
        return Ok(name.to_string());
    }
    // Search PATH using `which`
    let output = std::process::Command::new("which")
        .arg(name)
        .output()
        .with_context(|| format!("Failed to run `which {}`", name))?;
    if output.status.success() {
        let resolved = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !resolved.is_empty() {
            return Ok(resolved);
        }
    }
    bail!(
        "Binary '{}' not found on PATH (the jailer requires absolute paths)",
        name
    );
}

/// Firecracker microVM driver implementing the `ProcessDriver` trait.
pub struct FirecrackerDriver {
    /// Path to the firecracker binary.
    firecracker_binary: String,
    /// Path to the jailer binary (for warm pool VMs).
    jailer_binary: String,
    /// Path to the Linux kernel image.
    kernel_image_path: PathBuf,
    /// Base image thin LV handle.
    base_image: BaseImageHandle,
    /// LVM thin pool configuration for per-VM thin snapshots.
    lvm_config: dm_thin::LvmConfig,
    /// CNI networking manager.
    cni: CniManager,
    /// Guest gateway IP address.
    guest_gateway: String,
    /// Guest netmask.
    guest_netmask: String,
    /// Default vCPU count per VM.
    default_vcpu_count: u32,
    /// Default memory in MiB per VM.
    default_memory_mib: u64,
    /// Default rootfs size in bytes per VM.
    default_rootfs_size_bytes: u64,
    /// Directory for API sockets and VM metadata.
    state_dir: PathBuf,
    /// Directory for VM log files.
    log_dir: PathBuf,
    /// In-memory registry of running VMs.
    vms: Arc<Mutex<HashMap<String, VmState>>>,
    /// Warm VM pool (None if warm pool is not configured).
    warm_pool: Option<Arc<Mutex<warm_pool::WarmPool>>>,
}

impl FirecrackerDriver {
    /// Create a new Firecracker driver.
    ///
    /// Validates paths, creates directories, sets up the origin device,
    /// and recovers any VMs from a previous run.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        firecracker_binary: Option<String>,
        jailer_binary: Option<String>,
        kernel_image_path: String,
        default_rootfs_size_bytes: Option<u64>,
        base_rootfs_image: String,
        cni_network_name: String,
        cni_bin_path: Option<String>,
        guest_gateway: String,
        guest_netmask: Option<String>,
        default_vcpu_count: Option<u32>,
        default_memory_mib: Option<u64>,
        state_dir: PathBuf,
        log_dir: PathBuf,
        lvm_volume_group: String,
        lvm_thin_pool: String,
        warm_pool_config: Option<crate::config::WarmPoolConfig>,
    ) -> Result<Arc<Self>> {
        let firecracker_binary =
            resolve_binary_path(firecracker_binary.as_deref().unwrap_or("firecracker"))?;
        // Jailer is only required for warm pool VMs. If warm pool is not
        // configured, fall back to unresolved name (won't be used).
        let jailer_binary = if warm_pool_config.is_some() {
            resolve_binary_path(jailer_binary.as_deref().unwrap_or("jailer"))?
        } else {
            jailer_binary.unwrap_or_else(|| "jailer".to_string())
        };
        let kernel_path = PathBuf::from(&kernel_image_path);
        let rootfs_path = PathBuf::from(&base_rootfs_image);
        let cni_bin_path = cni_bin_path.unwrap_or_else(|| DEFAULT_CNI_BIN_PATH.to_string());
        let guest_netmask = guest_netmask.unwrap_or_else(|| DEFAULT_GUEST_NETMASK.to_string());
        let rootfs_size = default_rootfs_size_bytes.unwrap_or(DEFAULT_ROOTFS_SIZE_BYTES);
        let vcpu_count = default_vcpu_count.unwrap_or(DEFAULT_VCPU_COUNT);
        let memory_mib = default_memory_mib.unwrap_or(DEFAULT_MEMORY_MIB);

        // Validate paths
        if !kernel_path.exists() {
            bail!("Kernel image not found: {}", kernel_path.display());
        }
        if !rootfs_path.exists() {
            bail!("Base rootfs image not found: {}", rootfs_path.display());
        }

        // Create directories
        std::fs::create_dir_all(&state_dir)
            .with_context(|| format!("Failed to create state dir {}", state_dir.display()))?;
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("Failed to create log dir {}", log_dir.display()))?;

        let lvm_config = dm_thin::LvmConfig {
            volume_group: lvm_volume_group,
            thin_pool: lvm_thin_pool,
        };
        dm_thin::validate_lvm_config(&lvm_config).context("LVM thin pool validation failed")?;

        let cni = CniManager::new(cni_network_name, cni_bin_path);

        // --- Base image lifecycle ---
        //
        // The base image is a thin LV containing the rootfs. Each per-VM
        // volume is a native thin snapshot of this LV, with COW block sharing
        // and independent sizing.
        //
        // If the base image path has changed since last run, tear down the old
        // base LV first so setup_base_image() creates a fresh one.
        if let Ok(Some(old_meta)) = BaseImageMetadata::load(&state_dir) &&
            old_meta.base_image_path != base_rootfs_image
        {
            tracing::info!(
                old_image = %old_meta.base_image_path,
                new_image = %base_rootfs_image,
                "Base rootfs image changed, tearing down old base LV"
            );
            if !old_meta.lv_name.is_empty() &&
                let Err(e) = dm_thin::teardown_base_image(&old_meta.lv_name, &lvm_config)
            {
                tracing::warn!(error = ?e, "Failed to tear down old base LV (continuing)");
            }
            BaseImageMetadata::remove(&state_dir);
        }

        let base_image = dm_thin::setup_base_image(&rootfs_path, &lvm_config)
            .context("Failed to set up base image thin LV")?;

        // Persist base image metadata for recovery.
        let base_meta = BaseImageMetadata {
            base_image_path: base_rootfs_image.clone(),
            lv_name: base_image.lv_name.clone(),
        };
        base_meta.save(&state_dir)?;

        // Recover VMs from metadata files.
        let metadata_list = scan_metadata_files(&state_dir)?;
        let mut recovered_vms = HashMap::new();
        let mut dead_vms = Vec::new();

        for metadata in metadata_list {
            if is_firecracker_process(metadata.pid) {
                tracing::info!(
                    vm_id = %metadata.vm_id,
                    pid = metadata.pid,
                    lv_name = %metadata.lv_name,
                    "Recovered running Firecracker VM"
                );

                // Thin LVs persist in LVM metadata across dataplane
                // restarts — no reconnection needed.

                if metadata.labels.is_empty() {
                    tracing::info!(
                        vm_id = %metadata.vm_id,
                        "Recovered VM has no labels (pre-labels metadata), \
                         log streamer tracing fields will be empty"
                    );
                }
                let log_cancel = log_stream::spawn_log_streamer(
                    metadata.vm_id.clone(),
                    log_dir.clone(),
                    metadata.labels.clone(),
                );

                recovered_vms.insert(
                    metadata.handle_id.clone(),
                    VmState {
                        process: VmProcess::Recovered { pid: metadata.pid },
                        metadata,
                        log_cancel: Some(log_cancel),
                    },
                );
            } else {
                tracing::info!(
                    vm_id = %metadata.vm_id,
                    pid = metadata.pid,
                    "Found dead Firecracker VM, scheduling cleanup"
                );
                dead_vms.push(metadata);
            }
        }

        // Collect active VM IDs before moving recovered_vms into the mutex.
        let active_vm_ids: HashSet<String> = recovered_vms
            .values()
            .map(|vm| vm.metadata.vm_id.clone())
            .collect();

        // Set up warm pool if configured — keep the receiver for the replenishment
        // task.
        let (warm_pool, replenish_rx) = match warm_pool_config {
            Some(cfg) => {
                let (tx, rx) = warm_pool::WarmPool::create_replenish_channel();
                (
                    Some(Arc::new(Mutex::new(warm_pool::WarmPool::new(cfg, tx)))),
                    Some(rx),
                )
            }
            None => (None, None),
        };

        // Extract warm VM metadata for async recovery in the replenishment task.
        // We can't do async health checks here (sync fn), so we pull them out
        // of recovered_vms and pass them to start_replenishment.
        let warm_vm_candidates: Vec<VmMetadata> = if warm_pool.is_some() {
            let warm_handle_ids: Vec<String> = recovered_vms
                .iter()
                .filter(|(_, vm)| vm.metadata.is_warm)
                .map(|(id, _)| id.clone())
                .collect();

            let mut candidates = Vec::new();
            for handle_id in warm_handle_ids {
                let vm_state = recovered_vms.remove(&handle_id).unwrap();
                // Cancel log streamer — warm VMs don't need active streaming.
                if let Some(cancel) = &vm_state.log_cancel {
                    cancel.cancel();
                }
                candidates.push(vm_state.metadata);
            }

            if !candidates.is_empty() {
                tracing::info!(
                    count = candidates.len(),
                    "Found warm VMs from previous run, will health-check in background"
                );
            }
            candidates
        } else {
            Vec::new()
        };

        // Clean up dead VMs (those with metadata but whose process is gone).
        let driver = Self {
            firecracker_binary,
            jailer_binary,
            kernel_image_path: kernel_path,
            base_image,
            lvm_config,
            cni,
            guest_gateway,
            guest_netmask,
            default_vcpu_count: vcpu_count,
            default_memory_mib: memory_mib,
            default_rootfs_size_bytes: rootfs_size,
            state_dir,
            log_dir,
            vms: Arc::new(Mutex::new(recovered_vms)),
            warm_pool,
        };

        for metadata in &dead_vms {
            driver.cleanup_dead_vm(metadata);
        }

        // Clean up stale thin LVs from crashed VMs that lost their metadata files.
        dm_thin::cleanup_stale_devices(&active_vm_ids, &driver.lvm_config);

        // Clean up leaked network namespaces from crashed VMs.
        driver.cni.cleanup_orphaned_netns_sync(&active_vm_ids);

        let driver = Arc::new(driver);

        // Start warm pool replenishment in the background (non-blocking).
        // Passes recovered warm VM candidates for async health-check + pool insertion.
        if let (Some(pool), Some(rx)) = (&driver.warm_pool, replenish_rx) {
            driver.start_replenishment(Arc::clone(pool), rx, warm_vm_candidates);
        }

        Ok(driver)
    }

    /// Returns the state directory path (used for VM metadata persistence).
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }

    /// Clean up resources for a dead VM (used during recovery).
    ///
    /// Synchronous cleanup (files, cgroup, dm device) runs inline so
    /// resources are released before the function returns. LV destruction
    /// and CNI teardown are spawned as background tasks — they're
    /// idempotent and tolerant of interruption.
    fn cleanup_dead_vm(&self, metadata: &VmMetadata) {
        // Remove metadata file
        metadata.remove(&self.state_dir);

        // Remove socket file
        let _ = std::fs::remove_file(&metadata.socket_path);

        // Remove log files
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}.log", metadata.vm_id)));
        let _ = std::fs::remove_file(
            self.log_dir
                .join(format!("fc-{}-serial.log", metadata.vm_id)),
        );
        let _ = std::fs::remove_file(
            self.log_dir
                .join(format!("fc-{}-jailer.log", metadata.vm_id)),
        );

        // Clean up jailer chroot and cgroup
        self.cleanup_jailer_chroot(&metadata.vm_id);
        if let Some(ref cgroup_path) = metadata.cgroup_path {
            cgroup::CgroupHandle::from_path(PathBuf::from(cgroup_path)).remove();
        }

        // Remove dm device synchronously (releases reference to backing LVs
        // and must complete before LV destruction).
        if let Some(ref dm_name) = metadata.dm_device_name {
            dm_swap::remove_dm_device_force(dm_name);
        }

        // LV destruction and CNI teardown run in the background — they're
        // idempotent and safe to interrupt on shutdown.
        let lv_name = metadata.lv_name.clone();
        let empty_lv_name = metadata.empty_lv_name.clone();
        let vm_id = metadata.vm_id.clone();
        let lvm_config = self.lvm_config.clone();
        let cni_network_name = self.cni.network_name().to_string();
        let cni_bin_path = self.cni.cni_bin_path().to_string();

        tokio::spawn(async move {
            if !lv_name.is_empty() &&
                let Err(e) = dm_thin::destroy_snapshot_async(lv_name, lvm_config.clone()).await
            {
                tracing::warn!(
                    error = ?e,
                    "Failed to destroy thin LV for dead VM"
                );
            }

            if let Some(empty_lv) = empty_lv_name &&
                let Err(e) = dm_thin::destroy_snapshot_async(empty_lv, lvm_config).await
            {
                tracing::warn!(
                    error = ?e,
                    "Failed to destroy empty LV for dead warm VM"
                );
            }

            let cni = CniManager::new(cni_network_name, cni_bin_path);
            cni.teardown_network(&vm_id).await;
        });
    }

    /// Claim a pre-booted warm VM and configure it for the given container.
    ///
    /// Steps:
    /// 1. Create thin LV snapshot of user's image
    /// 2. Swap dm device backing from empty LV to user's snapshot
    /// 3. Adjust CPU via cgroup and memory via balloon
    /// 4. Call daemon Prepare RPC to mount image and set env
    async fn claim_warm_vm(
        &self,
        warm_vm: warm_pool::WarmVm,
        config: &ProcessConfig,
        handle_id: &str,
        start_time: Instant,
    ) -> Result<ProcessHandle> {
        let vm_id = &warm_vm.vm_id;

        // Remove the warm VM's original registry entry (registered during
        // boot_warm_vm under the warm handle_id). We'll re-insert under the
        // container's handle_id below.
        {
            let mut vms = self.vms.lock().await;
            if let Some(old) = vms.remove(&warm_vm.handle_id)
                && let Some(cancel) = &old.log_cancel
            {
                cancel.cancel();
            }
        }

        // Compute resource limits
        let cpu_millicores = config
            .resources
            .as_ref()
            .and_then(|r| r.cpu_millicores)
            .unwrap_or(self.default_vcpu_count as u64 * 1000);
        let memory_mib = config
            .resources
            .as_ref()
            .and_then(|r| r.memory_bytes)
            .map(|b| (b / (1024 * 1024)).max(128))
            .unwrap_or(self.default_memory_mib);

        let rootfs_size_bytes = config
            .resources
            .as_ref()
            .and_then(|r| r.disk_bytes)
            .unwrap_or(self.default_rootfs_size_bytes)
            .max(self.base_image.size_bytes);

        // 1. Create thin LV snapshot of user's image
        let snapshot = dm_thin::create_snapshot_async(
            self.base_image.lv_name.clone(),
            self.base_image.device_path.clone(),
            self.base_image.size_bytes,
            config.id.clone(),
            self.lvm_config.clone(),
            rootfs_size_bytes,
        )
        .await
        .context("Failed to create thin snapshot for warm VM claim")?;

        // 2. Swap dm device backing from empty LV to user's snapshot
        let size_sectors = dm_swap::bytes_to_sectors(snapshot.size_bytes);
        let dev_path_str = snapshot.device_path.to_string_lossy().into_owned();
        dm_swap::swap_backing_async(warm_vm.dm_device_name.clone(), dev_path_str, size_sectors)
            .await
            .context("Failed to swap dm device backing")?;

        // 3. Adjust balloon (give guest the requested memory)
        let balloon_size = warm_vm.max_memory_mib.saturating_sub(memory_mib);
        let api_client = api::FirecrackerApiClient::new(&warm_vm.socket_path);
        api_client
            .update_balloon(balloon_size)
            .await
            .context("Failed to update balloon")?;

        // 4. Set CPU limit via cgroup
        warm_vm
            .cgroup
            .set_cpu_limit(cpu_millicores as u32)
            .await
            .context("Failed to set cgroup CPU limit")?;

        // 5. Prepare daemon (mount image, set env vars, offline extra CPUs)
        let mut env_vars: Vec<(String, String)> = config.env.clone();
        if let Some(dns) = rootfs::read_host_dns() {
            env_vars.push(("DNS_NAMESERVERS".to_string(), dns));
        }

        let mount_config = proto_api::container_daemon_pb::MountConfig {
            device: "/dev/vdb".to_string(),
            device_mount_point: "/mnt/image".to_string(),
            overlay_mount_point: "/mnt/merged".to_string(),
            use_overlay: true,
        };

        // Round up millicores to whole CPUs, capped by the VM's max vCPUs.
        let num_cpus = cpu_millicores
            .div_ceil(1000)
            .min(warm_vm.max_vcpus as u64)
            .max(1) as u32;

        let mut daemon_client =
            crate::daemon_client::DaemonClient::connect(&warm_vm.daemon_addr).await?;
        daemon_client
            .prepare(env_vars, None, Some(mount_config), Some(num_cpus))
            .await
            .context("Daemon prepare failed")?;

        // 6. Build addresses and persist metadata
        let labels: HashMap<String, String> = config.labels.iter().cloned().collect();

        let metadata = VmMetadata {
            handle_id: handle_id.to_string(),
            vm_id: vm_id.clone(),
            pid: warm_vm.pid,
            lv_name: snapshot.lv_name.clone(),
            netns_name: warm_vm.netns_name.clone(),
            guest_ip: warm_vm.guest_ip.clone(),
            daemon_addr: warm_vm.daemon_addr.clone(),
            http_addr: warm_vm.http_addr.clone(),
            socket_path: warm_vm.socket_path.clone(),
            labels: labels.clone(),
            is_warm: false, // No longer warm — it's claimed
            max_memory_mib: Some(warm_vm.max_memory_mib),
            max_vcpus: Some(warm_vm.max_vcpus),
            dm_device_name: Some(warm_vm.dm_device_name.clone()),
            empty_lv_name: Some(warm_vm.empty_lv_name.clone()),
            cgroup_path: Some(warm_vm.cgroup.path_string()),
        };
        metadata.save(&self.state_dir)?;

        // Spawn log streamer
        let log_cancel =
            log_stream::spawn_log_streamer(vm_id.clone(), self.log_dir.clone(), labels);

        // Insert into VM registry
        self.vms.lock().await.insert(
            handle_id.to_string(),
            VmState {
                process: VmProcess::Recovered { pid: warm_vm.pid },
                metadata,
                log_cancel: Some(log_cancel),
            },
        );

        tracing::info!(
            vm_id = %vm_id,
            total_ms = start_time.elapsed().as_millis() as u64,
            "Warm VM claim complete"
        );

        Ok(ProcessHandle {
            id: handle_id.to_string(),
            daemon_addr: Some(warm_vm.daemon_addr),
            http_addr: Some(warm_vm.http_addr),
            container_ip: warm_vm.guest_ip,
        })
    }

    /// Boot a warm VM for the pool.
    ///
    /// This follows the same steps as the cold boot path but:
    /// - Boots with max resources (vCPUs, memory)
    /// - Configures balloon to inflate (take memory from guest)
    /// - Sets up dm-linear device backed by an empty LV for vdb
    /// - Sets idle CPU limit via jailer cgroup
    /// - Does NOT inject user-specific env or mount user images
    pub async fn boot_warm_vm(
        &self,
        warm_pool_config: &crate::config::WarmPoolConfig,
    ) -> Result<warm_pool::WarmVm> {
        let vm_id = format!("warm-{}", &uuid::Uuid::new_v4().to_string()[..12]);
        let handle_id = format!("fc-{}", vm_id);
        let start_time = Instant::now();

        let max_vcpus = warm_pool_config
            .max_vcpus
            .unwrap_or(self.default_vcpu_count);
        let max_memory_mib = warm_pool_config
            .max_memory_mib
            .unwrap_or(self.default_memory_mib);

        // Track allocated resources so we can clean up on any failure.
        // Each step sets a flag so the cleanup block knows what to tear down.
        let vm_id_for_cleanup = vm_id.clone();
        let mut snapshot_lv: Option<String> = None;
        let mut cni_setup = false;
        let mut empty_lv_name: Option<String> = None;
        let mut dm_name_created: Option<String> = None;
        let mut fc_pid: Option<u32> = None;

        let result: Result<warm_pool::WarmVm> = async {
            // 1. Create thin snapshot of base image
            let snapshot = dm_thin::create_snapshot_async(
                self.base_image.lv_name.clone(),
                self.base_image.device_path.clone(),
                self.base_image.size_bytes,
                vm_id.clone(),
                self.lvm_config.clone(),
                self.default_rootfs_size_bytes,
            )
            .await
            .context("Failed to create thin snapshot for warm VM")?;
            snapshot_lv = Some(snapshot.lv_name.clone());

            // 2. Inject rootfs (daemon binary + init script, no user env)
            let daemon_binary =
                crate::daemon_binary::get_daemon_path().context("Daemon binary not available")?;
            let dns_env = rootfs::read_host_dns()
                .map(|dns| vec![("DNS_NAMESERVERS".to_string(), dns)])
                .unwrap_or_default();
            rootfs::inject_rootfs(&snapshot.device_path, daemon_binary, &dns_env, &vm_id).await?;

            // 3. Setup CNI networking
            let cni_result = self.cni.setup_network(&vm_id).await?;
            cni_setup = true;

            // 4. Create empty LV + dm device for vdb.  Size matches the default
            //    rootfs size so the guest virtio-blk sees the right capacity
            //    from boot and claim-time snapshots (which use the same default)
            //    fit without a dm table resize.  Thin-provisioned → near-zero
            //    actual disk use.
            let empty_lv = dm_thin::create_empty_lv_async(
                vm_id.clone(),
                self.lvm_config.clone(),
                self.default_rootfs_size_bytes,
            )
            .await
            .context("Failed to create empty LV for warm VM vdb")?;
            empty_lv_name = Some(empty_lv.lv_name.clone());

            let dm_name = dm_swap::dm_device_name(&vm_id);
            let empty_sectors = dm_swap::bytes_to_sectors(empty_lv.size_bytes);
            let empty_dev_path = empty_lv.device_path.to_string_lossy().into_owned();
            let dm_device_path = tokio::task::spawn_blocking({
                let vm_id = vm_id.clone();
                let empty_dev = empty_dev_path.clone();
                move || dm_swap::create_dm_device(&vm_id, &empty_dev, empty_sectors)
            })
            .await
            .context("dm device creation task panicked")?
            .context("Failed to create dm device for warm VM")?;
            dm_name_created = Some(dm_name.clone());

            // 5. Spawn Firecracker via jailer
            cgroup::ensure_parent_cgroup(&warm_pool_config.parent_cgroup)?;
            let chroot_base = PathBuf::from("/srv/jailer");
            std::fs::create_dir_all(&chroot_base)?;
            let chroot_dir = chroot_base.join("firecracker").join(&vm_id).join("root");

            let serial_path = self.log_dir.join(format!("fc-{}-serial.log", vm_id));
            let serial_file = std::fs::File::create(&serial_path)
                .or_else(|_| std::fs::File::create("/dev/null"))?;

            let boot_args = self.build_boot_args(&cni_result.guest_ip);

            let netns_path = format!("/var/run/netns/{}", cni_result.netns_name);
            let chroot_base_str = chroot_base.to_string_lossy().into_owned();
            let idle_cpu_quota = format!(
                "cpu.max={} 100000",
                warm_pool_config.idle_cpu_millicores as u64 * 100
            );

            let stderr_path = self.log_dir.join(format!("fc-{}-jailer.log", vm_id));
            let stderr_file = std::fs::File::create(&stderr_path)
                .or_else(|_| std::fs::File::create("/dev/null"))?;

            let _child = tokio::process::Command::new(&self.jailer_binary)
                .args([
                    "--id",
                    &vm_id,
                    "--exec-file",
                    &self.firecracker_binary,
                    "--uid",
                    "0",
                    "--gid",
                    "0",
                    "--cgroup-version",
                    "2",
                    "--cgroup",
                    &idle_cpu_quota,
                    "--parent-cgroup",
                    &warm_pool_config.parent_cgroup,
                    "--netns",
                    &netns_path,
                    "--new-pid-ns",
                    "--chroot-base-dir",
                    &chroot_base_str,
                    "--", // separator for firecracker args
                    "--api-sock",
                    "/run/firecracker.socket",
                ])
                .stdin(Stdio::null())
                .stdout(serial_file)
                .stderr(stderr_file)
                .kill_on_drop(false)
                .process_group(0)
                .spawn()
                .context("Failed to spawn jailer for warm VM")?;

            // The jailer with --new-pid-ns forks: the parent exits after exec-ing
            // the child into the new PID namespace. Wait briefly to let it set up,
            // then check stderr for errors.
            tokio::time::sleep(Duration::from_millis(200)).await;
            let stderr_content = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            if !stderr_content.is_empty() {
                bail!("Jailer failed: {}", stderr_content.trim());
            }

            // The real Firecracker PID is in the chroot (written by jailer).
            let pid_file = chroot_dir.join("firecracker.pid");
            let pid: u32 = std::fs::read_to_string(&pid_file)
                .with_context(|| format!("Failed to read PID file {}", pid_file.display()))?
                .trim()
                .parse()
                .with_context(|| format!("Invalid PID in {}", pid_file.display()))?;
            fc_pid = Some(pid);

            // The API socket is inside the chroot.
            let socket_path = chroot_dir
                .join("run")
                .join("firecracker.socket")
                .to_string_lossy()
                .into_owned();

            // 6. Wait for API socket
            let api_client = FirecrackerApiClient::new(&socket_path);
            api_client
                .wait_for_socket(API_SOCKET_TIMEOUT, API_SOCKET_POLL_INTERVAL)
                .await?;

            // Hard-link device nodes and kernel into the chroot.
            let kernel_chroot = chroot_dir.join("kernel");
            let rootfs_chroot = chroot_dir.join("rootfs");
            let dm_chroot = chroot_dir.join("vdb");

            Self::link_or_copy(&self.kernel_image_path, &kernel_chroot)?;
            Self::link_device_node(&snapshot.device_path, &rootfs_chroot)?;
            Self::link_device_node(&PathBuf::from(&dm_device_path), &dm_chroot)?;

            // 7. Configure VM via API (paths are relative to chroot)
            api_client
                .configure_boot_source("/kernel", &boot_args)
                .await?;
            api_client.configure_rootfs("/rootfs").await?;
            api_client
                .configure_machine(max_vcpus, max_memory_mib)
                .await?;
            api_client
                .configure_network(&cni_result.tap_device, &cni_result.guest_mac)
                .await?;

            // Configure balloon: inflate to take all memory (idle VM needs minimal memory)
            let balloon_inflate = max_memory_mib.saturating_sub(warm_pool_config.idle_memory_mib);
            api_client.configure_balloon(balloon_inflate, true).await?;

            // Configure secondary drive (vdb) backed by dm device
            api_client
                .configure_secondary_drive("vdb", "/vdb", false, "Unsafe")
                .await?;

            // Start the VM
            api_client.start_instance().await?;

            // 8. Wait for daemon to be healthy
            let daemon_addr = format!("{}:{}", cni_result.guest_ip, DAEMON_GRPC_PORT);
            let http_addr = format!("{}:{}", cni_result.guest_ip, DAEMON_HTTP_PORT);

            let mut daemon_client = crate::daemon_client::DaemonClient::connect_with_retry(
                &daemon_addr,
                Duration::from_secs(30),
            )
            .await?;
            daemon_client
                .wait_for_ready(Duration::from_secs(30))
                .await?;

            // Cgroup was created by the jailer — derive handle for runtime adjustments.
            let cgroup = cgroup::CgroupHandle::from_jailer(&warm_pool_config.parent_cgroup, &vm_id);

            // 9. Persist metadata
            let metadata = VmMetadata {
                handle_id: handle_id.clone(),
                vm_id: vm_id.clone(),
                pid,
                lv_name: snapshot.lv_name.clone(),
                netns_name: cni_result.netns_name.clone(),
                guest_ip: cni_result.guest_ip.clone(),
                daemon_addr: daemon_addr.clone(),
                http_addr: http_addr.clone(),
                socket_path: socket_path.clone(),
                labels: HashMap::new(),
                is_warm: true,
                max_memory_mib: Some(max_memory_mib),
                max_vcpus: Some(max_vcpus),
                dm_device_name: Some(dm_name.clone()),
                empty_lv_name: Some(empty_lv.lv_name.clone()),
                cgroup_path: Some(cgroup.path_string()),
            };
            metadata.save(&self.state_dir)?;

            // Spawn log streamer
            let log_cancel =
                log_stream::spawn_log_streamer(vm_id.clone(), self.log_dir.clone(), HashMap::new());

            // Register in VM registry (so recovery can find it).
            // The jailer forks with --new-pid-ns, so we only have the PID
            // (not a Child handle). Use Recovered mode.
            self.vms.lock().await.insert(
                handle_id.clone(),
                VmState {
                    process: VmProcess::Recovered { pid },
                    metadata,
                    log_cancel: Some(log_cancel),
                },
            );

            tracing::info!(
                vm_id = %vm_id,
                max_vcpus,
                max_memory_mib,
                total_ms = start_time.elapsed().as_millis() as u64,
                "Warm VM booted successfully"
            );

            Ok(warm_pool::WarmVm {
                handle_id,
                vm_id,
                socket_path,
                daemon_addr,
                http_addr,
                guest_ip: cni_result.guest_ip,
                lv_name: snapshot.lv_name,
                netns_name: cni_result.netns_name,
                dm_device_name: dm_name,
                empty_lv_name: empty_lv.lv_name,
                cgroup,
                max_memory_mib,
                max_vcpus,
                pid,
            })
        }
        .await;

        // On failure, clean up any resources that were allocated.
        if result.is_err() {
            tracing::warn!(vm_id = %vm_id_for_cleanup, "Warm VM boot failed, cleaning up partial resources");
            // Kill the FC process if it was started.
            if let Some(pid) = fc_pid {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGKILL,
                );
                let mut proc = VmProcess::Recovered { pid };
                proc.wait_for_exit().await;
            } else {
                // PID file read failed but jailer may have spawned a child FC
                // process. Kill any processes in the cgroup to avoid orphans.
                let cgroup = cgroup::CgroupHandle::from_jailer(
                    &warm_pool_config.parent_cgroup,
                    &vm_id_for_cleanup,
                );
                cgroup.kill_all_processes();
            }
            if let Some(dm) = &dm_name_created {
                dm_swap::remove_dm_device_force(dm);
            }
            if let Some(lv) = empty_lv_name {
                let _ = dm_thin::destroy_snapshot_async(lv, self.lvm_config.clone()).await;
            }
            if cni_setup {
                self.cni.teardown_network(&vm_id_for_cleanup).await;
            }
            if let Some(lv) = snapshot_lv {
                let _ = dm_thin::destroy_snapshot_async(lv, self.lvm_config.clone()).await;
            }
            self.cleanup_jailer_chroot(&vm_id_for_cleanup);
            // Remove the cgroup directory created by the jailer.
            cgroup::CgroupHandle::from_jailer(&warm_pool_config.parent_cgroup, &vm_id_for_cleanup)
                .remove();
        }

        result
    }

    /// Build kernel boot args for a VM with the given guest IP.
    fn build_boot_args(&self, guest_ip: &str) -> String {
        format!(
            "console=ttyS0 reboot=k panic=1 pci=off \
             ip={}::{}:{}::eth0:off \
             init=/sbin/indexify-init",
            guest_ip, self.guest_gateway, self.guest_netmask,
        )
    }

    /// Build daemon gRPC and HTTP addresses from a guest IP.
    fn build_daemon_addrs(guest_ip: &str) -> (String, String) {
        (
            format!("{}:{}", guest_ip, DAEMON_GRPC_PORT),
            format!("{}:{}", guest_ip, DAEMON_HTTP_PORT),
        )
    }

    /// Hard-link a regular file into the jailer chroot. Falls back to copy
    /// if hard-linking fails (e.g., cross-device).
    fn link_or_copy(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::hard_link(src, dst)
            .or_else(|_| std::fs::copy(src, dst).map(|_| ()))
            .with_context(|| format!("Failed to link/copy {} -> {}", src.display(), dst.display()))
    }

    /// Create a device node in the jailer chroot that mirrors the given
    /// block device. Uses `mknod` with the same major/minor as the source.
    fn link_device_node(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let meta =
            std::fs::metadata(src).with_context(|| format!("stat {} for mknod", src.display()))?;
        let rdev = meta.rdev();
        let major = nix::sys::stat::major(rdev);
        let minor = nix::sys::stat::minor(rdev);
        let dev = nix::sys::stat::makedev(major, minor);
        nix::sys::stat::mknod(
            dst,
            nix::sys::stat::SFlag::S_IFBLK,
            nix::sys::stat::Mode::from_bits_truncate(0o660),
            dev,
        )
        .with_context(|| {
            format!(
                "mknod {} ({}:{}) for {}",
                dst.display(),
                major,
                minor,
                src.display()
            )
        })
    }

    /// Clean up a jailer chroot directory for a VM.
    fn cleanup_jailer_chroot(&self, vm_id: &str) {
        let chroot_dir = PathBuf::from("/srv/jailer").join("firecracker").join(vm_id);
        if chroot_dir.exists() {
            let _ = std::fs::remove_dir_all(&chroot_dir);
        }
    }

    /// Spawn the warm pool replenishment background task.
    ///
    /// This task watches for replenishment signals and boots new warm VMs
    /// to maintain the target pool size. Non-blocking — the dataplane
    /// starts accepting work immediately.
    ///
    /// If `warm_vm_candidates` is non-empty (restart recovery), health-checks
    /// each candidate and pushes healthy ones into the pool before the initial
    /// fill, so we don't boot duplicates.
    fn start_replenishment(
        self: &Arc<Self>,
        warm_pool: Arc<Mutex<warm_pool::WarmPool>>,
        mut replenish_rx: mpsc::Receiver<()>,
        warm_vm_candidates: Vec<VmMetadata>,
    ) {
        let driver = Arc::clone(self);
        let pool = Arc::clone(&warm_pool);

        tokio::spawn(async move {
            // Get config once
            let config = {
                let p = pool.lock().await;
                p.config().clone()
            };

            // Phase 0: recover warm VMs from previous run.
            if !warm_vm_candidates.is_empty() {
                Self::recover_warm_vms(&driver, &pool, warm_vm_candidates).await;
            }

            // Initial fill (accounts for any recovered VMs already in the pool).
            tracing::info!(
                target_count = config.target_count,
                "Warm pool: starting initial fill"
            );
            Self::replenish_pool(&driver, &pool, &config).await;

            // Replenishment loop: event-driven with periodic safety-net timer.
            // Wakes on claim/destroy signals OR every 30s (whichever comes first).
            let mut failure_reset = tokio::time::interval(Duration::from_secs(60));
            failure_reset.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    // Event-driven: claim or destroy triggered replenishment
                    msg = replenish_rx.recv() => {
                        if msg.is_none() {
                            // Channel closed — warm pool dropped, stop task
                            tracing::info!("Warm pool replenishment channel closed, stopping");
                            break;
                        }
                        // Drain any extra signals that arrived while we were booting
                        while replenish_rx.try_recv().is_ok() {}
                    }
                    // Safety-net: periodic replenishment check
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                    // Periodic failure reset to allow retry after backoff
                    _ = failure_reset.tick() => {
                        let mut p = pool.lock().await;
                        p.reset_failures();
                    }
                }

                Self::replenish_pool(&driver, &pool, &config).await;
            }
        });
    }

    /// Run one round of replenishment.
    async fn replenish_pool(
        driver: &Arc<Self>,
        pool: &Arc<Mutex<warm_pool::WarmPool>>,
        config: &crate::config::WarmPoolConfig,
    ) {
        let needed = {
            let p = pool.lock().await;
            p.replenish_count()
        };

        if needed == 0 {
            return;
        }

        tracing::info!(count = needed, "Warm pool: booting VMs");

        {
            let mut p = pool.lock().await;
            p.start_boots(needed);
        }

        let mut handles = Vec::with_capacity(needed);
        for _ in 0..needed {
            let driver = Arc::clone(driver);
            let pool = Arc::clone(pool);
            let config = config.clone();
            handles.push(tokio::spawn(async move {
                match driver.boot_warm_vm(&config).await {
                    Ok(vm) => {
                        let mut p = pool.lock().await;
                        p.push_warm(vm);
                        p.boot_completed();
                    }
                    Err(e) => {
                        tracing::warn!(error = ?e, "Failed to boot warm VM");
                        let mut p = pool.lock().await;
                        p.record_boot_failure();
                    }
                }
            }));
        }

        // Wait for all boots to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Get the warm pool reference (for resource reporting).
    pub fn warm_pool(&self) -> Option<&Arc<Mutex<warm_pool::WarmPool>>> {
        self.warm_pool.as_ref()
    }

    /// Clean up all resources for a VM after it has been killed.
    async fn cleanup_vm(&self, handle_id: &str, vm_id: &str, lv_name: &str, socket_path: &str) {
        // Cancel log streamer and remove from registry.
        {
            let mut vms = self.vms.lock().await;
            if let Some(vm) = vms.remove(handle_id) &&
                let Some(cancel) = &vm.log_cancel
            {
                cancel.cancel();
            }
        }

        // Destroy thin LV.
        if let Err(e) =
            dm_thin::destroy_snapshot_async(lv_name.to_string(), self.lvm_config.clone()).await
        {
            tracing::warn!(lv_name, error = ?e, "Failed to destroy thin LV");
        }

        // Teardown CNI networking
        self.cni.teardown_network(vm_id).await;

        // Remove socket file
        let _ = std::fs::remove_file(socket_path);

        // Remove metadata file
        let metadata_path = self.state_dir.join(format!("fc-{}.json", vm_id));
        let _ = std::fs::remove_file(metadata_path);

        // Remove log files (including jailer stderr log).
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}.log", vm_id)));
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}-serial.log", vm_id)));
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}-jailer.log", vm_id)));

        // Clean up jailer chroot directory (if it exists).
        self.cleanup_jailer_chroot(vm_id);
    }

    /// Recover warm VMs from a previous run.
    ///
    /// Health-checks all candidates concurrently (5s timeout each), pushes
    /// healthy VMs into the pool, and destroys unhealthy ones. Called once
    /// at startup before the initial replenishment fill.
    async fn recover_warm_vms(
        driver: &Arc<Self>,
        pool: &Arc<Mutex<warm_pool::WarmPool>>,
        candidates: Vec<VmMetadata>,
    ) {
        let total = candidates.len();

        // Validate fields and split into valid candidates vs. immediate cleanup.
        let mut valid = Vec::new();
        for meta in candidates {
            let has_fields = meta.dm_device_name.is_some() &&
                meta.empty_lv_name.is_some() &&
                meta.max_memory_mib.is_some() &&
                meta.max_vcpus.is_some();

            if has_fields {
                valid.push(meta);
            } else {
                tracing::warn!(
                    vm_id = %meta.vm_id,
                    "Warm VM missing required metadata fields, destroying"
                );
                driver.cleanup_dead_vm(&meta);
            }
        }

        // Health-check all valid candidates concurrently.
        let health_futures: Vec<_> = valid
            .iter()
            .map(|meta| {
                let addr = meta.daemon_addr.clone();
                async move {
                    match crate::daemon_client::DaemonClient::connect_with_retry(
                        &addr,
                        Duration::from_secs(5),
                    )
                    .await
                    {
                        Ok(mut client) => client.health().await.unwrap_or(false),
                        Err(_) => false,
                    }
                }
            })
            .collect();

        let results = futures_util::future::join_all(health_futures).await;

        let mut recovered = 0usize;
        for (meta, healthy) in valid.into_iter().zip(results) {
            if healthy {
                let cgroup = match &meta.cgroup_path {
                    Some(p) => cgroup::CgroupHandle::from_path(PathBuf::from(p)),
                    None => cgroup::CgroupHandle::from_jailer("indexify", &meta.vm_id),
                };
                let warm_vm = warm_pool::WarmVm {
                    handle_id: meta.handle_id.clone(),
                    vm_id: meta.vm_id.clone(),
                    socket_path: meta.socket_path.clone(),
                    daemon_addr: meta.daemon_addr.clone(),
                    http_addr: meta.http_addr.clone(),
                    guest_ip: meta.guest_ip.clone(),
                    lv_name: meta.lv_name.clone(),
                    netns_name: meta.netns_name.clone(),
                    dm_device_name: meta.dm_device_name.unwrap(),
                    empty_lv_name: meta.empty_lv_name.unwrap(),
                    cgroup,
                    max_memory_mib: meta.max_memory_mib.unwrap(),
                    max_vcpus: meta.max_vcpus.unwrap(),
                    pid: meta.pid,
                };
                tracing::info!(vm_id = %warm_vm.vm_id, "Recovered warm VM into pool");
                pool.lock().await.push_warm(warm_vm);
                recovered += 1;
            } else {
                tracing::info!(
                    vm_id = %meta.vm_id,
                    "Warm VM unhealthy after restart, destroying"
                );
                driver.cleanup_dead_vm(&meta);
            }
        }

        if recovered > 0 {
            let p = pool.lock().await;
            tracing::info!(
                recovered,
                total,
                target = p.config().target_count,
                "Warm pool recovery complete"
            );
        }
    }
}

#[async_trait]
impl ProcessDriver for FirecrackerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let vm_id = config.id.clone();
        let handle_id = format!("fc-{}", vm_id);
        let start_time = Instant::now();

        // --- Warm pool fast path ---
        // Try to claim a pre-booted VM from the warm pool.
        if let Some(warm_pool) = &self.warm_pool {
            let claimed = {
                let mut pool = warm_pool.lock().await;
                pool.try_claim()
            };

            if let Some(warm_vm) = claimed {
                // Capture warm VM info for cleanup before moving into claim.
                let warm_vm_id = warm_vm.vm_id.clone();
                let warm_lv_name = warm_vm.lv_name.clone();
                let warm_socket_path = warm_vm.socket_path.clone();
                let warm_dm_device_name = warm_vm.dm_device_name.clone();
                let warm_empty_lv_name = warm_vm.empty_lv_name.clone();
                let warm_pid = warm_vm.pid;

                match self
                    .claim_warm_vm(warm_vm, &config, &handle_id, start_time)
                    .await
                {
                    Ok(handle) => return Ok(handle),
                    Err(e) => {
                        tracing::warn!(
                            error = ?e,
                            vm_id = %vm_id,
                            warm_vm_id = %warm_vm_id,
                            "Warm VM claim failed, destroying warm VM and falling back to cold boot"
                        );
                        // The claim may have partially mutated the warm VM
                        // (e.g., dm-swap succeeded but balloon/cgroup/prepare
                        // failed). We must fully destroy the warm VM so the
                        // cold boot fallback can create resources from scratch.
                        //
                        // Kill the Firecracker process and wait for exit so
                        // subsequent dmsetup/lvremove don't fail with EBUSY.
                        let _ = nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(warm_pid as i32),
                            nix::sys::signal::Signal::SIGKILL,
                        );
                        let mut proc = VmProcess::Recovered { pid: warm_pid };
                        proc.wait_for_exit().await;
                        // Remove any VM registry entry that claim_warm_vm may
                        // have inserted (under the container's handle_id). The
                        // original warm entry was already removed at the start
                        // of claim_warm_vm.
                        {
                            let mut vms = self.vms.lock().await;
                            if let Some(vm) = vms.remove(&handle_id)
                                && let Some(cancel) = &vm.log_cancel
                            {
                                cancel.cancel();
                            }
                        }
                        // Remove dm device (releases reference to backing LVs).
                        dm_swap::remove_dm_device_async(warm_dm_device_name).await;
                        // Remove the claim snapshot LV (may or may not exist).
                        // This MUST succeed before the cold boot fallback below,
                        // which will create a new LV with the same name. If we
                        // can't remove it, bail — cold boot would fail anyway.
                        let claim_lv = format!("indexify-vm-{}", vm_id);
                        if let Err(e) =
                            dm_thin::destroy_snapshot_async(claim_lv, self.lvm_config.clone()).await
                        {
                            // Still clean up remaining warm VM resources.
                            let _ = dm_thin::destroy_snapshot_async(
                                warm_lv_name,
                                self.lvm_config.clone(),
                            )
                            .await;
                            let _ = dm_thin::destroy_snapshot_async(
                                warm_empty_lv_name,
                                self.lvm_config.clone(),
                            )
                            .await;
                            self.cni.teardown_network(&warm_vm_id).await;
                            self.cleanup_jailer_chroot(&warm_vm_id);
                            let _ = std::fs::remove_file(&warm_socket_path);
                            let _ = std::fs::remove_file(
                                self.state_dir.join(format!("fc-{}.json", warm_vm_id)),
                            );
                            if let Some(wp) = &self.warm_pool {
                                wp.lock().await.notify_destroyed();
                            }
                            return Err(e).context(
                                "Failed to destroy claim snapshot LV after warm claim failure; \
                                 cannot fall back to cold boot",
                            );
                        }
                        // Remove the warm VM's original rootfs LV.
                        if let Err(e) =
                            dm_thin::destroy_snapshot_async(warm_lv_name, self.lvm_config.clone())
                                .await
                        {
                            tracing::warn!(error = ?e, "Failed to destroy warm VM rootfs LV");
                        }
                        // Remove the empty LV.
                        if let Err(e) = dm_thin::destroy_snapshot_async(
                            warm_empty_lv_name,
                            self.lvm_config.clone(),
                        )
                        .await
                        {
                            tracing::warn!(error = ?e, "Failed to destroy warm VM empty LV");
                        }
                        // Teardown CNI networking.
                        self.cni.teardown_network(&warm_vm_id).await;
                        // Remove jailer chroot directory.
                        self.cleanup_jailer_chroot(&warm_vm_id);
                        // Remove socket and metadata files.
                        let _ = std::fs::remove_file(&warm_socket_path);
                        let _ = std::fs::remove_file(
                            self.state_dir.join(format!("fc-{}.json", warm_vm_id)),
                        );
                        // Notify warm pool that a VM was destroyed.
                        if let Some(wp) = &self.warm_pool {
                            let mut pool = wp.lock().await;
                            pool.notify_destroyed();
                        }
                    }
                }
            }
        }

        // --- Cold boot path ---
        // Extract tracing labels in a single pass (before config.labels is consumed).
        let (mut pool, mut namespace, mut container_id, mut sandbox_id) =
            (String::new(), String::new(), String::new(), String::new());
        for (k, v) in &config.labels {
            match k.as_str() {
                "pool_id" => pool.clone_from(v),
                "namespace" => namespace.clone_from(v),
                "container_id" => container_id.clone_from(v),
                "sandbox_id" => sandbox_id.clone_from(v),
                _ => {}
            }
        }

        // Use per-container disk size if provided, otherwise fall back to the
        // driver default. Clamp to at least the base image size so the thin
        // snapshot is never smaller than the base.
        let rootfs_size_bytes = config
            .resources
            .as_ref()
            .and_then(|r| r.disk_bytes)
            .unwrap_or(self.default_rootfs_size_bytes)
            .max(self.base_image.size_bytes);

        // Detect restore path: image ends with .delta (delta from snapshotter).
        let is_restore = config
            .image
            .as_ref()
            .is_some_and(|img| img.ends_with(".delta"));

        let raw_cpu_millicores = config.resources.as_ref().and_then(|r| r.cpu_millicores);
        let raw_memory_bytes = config.resources.as_ref().and_then(|r| r.memory_bytes);
        let raw_disk_bytes = config.resources.as_ref().and_then(|r| r.disk_bytes);

        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            is_restore,
            raw_cpu_millicores = ?raw_cpu_millicores,
            raw_memory_bytes = ?raw_memory_bytes,
            raw_disk_bytes = ?raw_disk_bytes,
            rootfs_size_bytes,
            base_image_size_bytes = self.base_image.size_bytes,
            "Starting VM creation"
        );

        // 1. Create thin snapshot for this VM.
        let snapshot = if is_restore {
            let delta_file = PathBuf::from(config.image.as_ref().unwrap());
            // Restore path: create thin snapshot of base, apply delta blocks.
            dm_thin::create_snapshot_from_delta_async(
                self.base_image.lv_name.clone(),
                self.base_image.device_path.clone(),
                self.base_image.size_bytes,
                vm_id.clone(),
                self.lvm_config.clone(),
                delta_file,
                rootfs_size_bytes,
            )
            .await
            .with_context(|| {
                format!("Failed to create thin snapshot from delta for VM {}", vm_id)
            })?
        } else {
            // Normal path: create thin snapshot of the base image.
            dm_thin::create_snapshot_async(
                self.base_image.lv_name.clone(),
                self.base_image.device_path.clone(),
                self.base_image.size_bytes,
                vm_id.clone(),
                self.lvm_config.clone(),
                rootfs_size_bytes,
            )
            .await
            .with_context(|| format!("Failed to create thin snapshot for VM {}", vm_id))?
        };
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            elapsed_ms = start_time.elapsed().as_millis() as u64,
            lv_name = %snapshot.lv_name,
            rootfs_size_bytes,
            is_restore,
            "Step 1: thin snapshot created"
        );

        // 2. Inject rootfs files into the snapshot.
        let step2 = Instant::now();
        let mut env_vars = config.env.clone();
        if let Some(dns) = rootfs::read_host_dns() {
            env_vars.push(("DNS_NAMESERVERS".to_string(), dns));
        }
        if is_restore {
            // Restore path: snapshot already contains daemon + init script from
            // the original VM. Only inject the env file (secrets/DNS may differ
            // on this host).
            if let Err(e) = rootfs::inject_env_only(&snapshot.device_path, &env_vars, &vm_id).await
            {
                let _ = dm_thin::destroy_snapshot_async(
                    snapshot.lv_name.clone(),
                    self.lvm_config.clone(),
                )
                .await;
                return Err(e.context("Failed to inject env vars into restored snapshot"));
            }
        } else {
            // Normal path: inject daemon binary, init script, and env vars.
            let daemon_binary =
                crate::daemon_binary::get_daemon_path().context("Daemon binary not available")?;
            if let Err(e) =
                rootfs::inject_rootfs(&snapshot.device_path, daemon_binary, &env_vars, &vm_id).await
            {
                let _ = dm_thin::destroy_snapshot_async(
                    snapshot.lv_name.clone(),
                    self.lvm_config.clone(),
                )
                .await;
                return Err(e.context("Failed to inject rootfs"));
            }
        }
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            elapsed_ms = step2.elapsed().as_millis() as u64,
            is_restore,
            "Step 2: rootfs injection complete"
        );

        // 3. Setup CNI networking
        let step3 = Instant::now();
        let cni_result = match self.cni.setup_network(&vm_id).await {
            Ok(result) => result,
            Err(e) => {
                let _ = dm_thin::destroy_snapshot_async(
                    snapshot.lv_name.clone(),
                    self.lvm_config.clone(),
                )
                .await;
                return Err(e.context("Failed to setup CNI networking"));
            }
        };
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            elapsed_ms = step3.elapsed().as_millis() as u64,
            guest_ip = %cni_result.guest_ip,
            "Step 3: CNI networking setup complete"
        );

        // 4. Compute resource limits
        let vcpus = config
            .resources
            .as_ref()
            .and_then(|r| r.cpu_millicores)
            .map(|m| (m / 1000).max(1) as u32)
            .unwrap_or(self.default_vcpu_count);
        let memory_mib = config
            .resources
            .as_ref()
            .and_then(|r| r.memory_bytes)
            .map(|b| (b / (1024 * 1024)).max(128))
            .unwrap_or(self.default_memory_mib);

        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            vcpus,
            memory_mib,
            rootfs_size_bytes,
            "Step 4: resource limits computed"
        );

        // 5. Build kernel boot args
        let boot_args = self.build_boot_args(&cni_result.guest_ip);

        // 6. Spawn Firecracker process
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            "Step 5-6: spawning Firecracker process"
        );
        let step6 = Instant::now();
        let socket_path = self
            .state_dir
            .join(format!("fc-{}.sock", vm_id))
            .to_string_lossy()
            .into_owned();
        let log_path = self
            .log_dir
            .join(format!("fc-{}.log", vm_id))
            .to_string_lossy()
            .into_owned();

        // Remove stale socket if present
        let _ = std::fs::remove_file(&socket_path);

        let serial_path = self.log_dir.join(format!("fc-{}-serial.log", vm_id));
        let serial_file = std::fs::File::create(&serial_path)
            .or_else(|_| std::fs::File::create("/dev/null"))
            .with_context(|| {
                format!(
                    "Failed to create serial log file {} or /dev/null fallback",
                    serial_path.display()
                )
            })?;

        let child = match tokio::process::Command::new("ip")
            .args([
                "netns",
                "exec",
                &cni_result.netns_name,
                &self.firecracker_binary,
                "--api-sock",
                &socket_path,
                "--log-path",
                &log_path,
            ])
            .stdin(Stdio::null())
            .stdout(serial_file)
            .stderr(Stdio::null())
            .kill_on_drop(false)
            // Put Firecracker in its own process group so terminal Ctrl+C
            // (SIGINT to the foreground pgrp) only reaches the dataplane,
            // not the child VM processes.  stdin must be /dev/null too —
            // Firecracker reads stdin for serial console input, and a
            // background pgrp reading from the terminal receives SIGTTIN.
            .process_group(0)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                self.cni.teardown_network(&vm_id).await;
                let _ = dm_thin::destroy_snapshot_async(
                    snapshot.lv_name.clone(),
                    self.lvm_config.clone(),
                )
                .await;
                return Err(anyhow::anyhow!("Failed to spawn firecracker: {}", e));
            }
        };

        let fc_pid = child.id().unwrap_or(0);
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            pid = fc_pid,
            elapsed_ms = step6.elapsed().as_millis() as u64,
            "Step 6: Firecracker process spawned"
        );

        // 7. Spawn log streamer
        let labels: HashMap<String, String> = config.labels.into_iter().collect();
        let log_cancel =
            log_stream::spawn_log_streamer(vm_id.clone(), self.log_dir.clone(), labels.clone());

        // 8. Wait for API socket
        let step8 = Instant::now();
        let api_client = FirecrackerApiClient::new(&socket_path);
        if let Err(e) = api_client
            .wait_for_socket(API_SOCKET_TIMEOUT, API_SOCKET_POLL_INTERVAL)
            .await
        {
            log_cancel.cancel();
            let mut child = child;
            let _ = child.kill().await;
            self.cni.teardown_network(&vm_id).await;
            let _ =
                dm_thin::destroy_snapshot_async(snapshot.lv_name.clone(), self.lvm_config.clone())
                    .await;
            let _ = std::fs::remove_file(&socket_path);
            return Err(e.context("Firecracker API socket not ready"));
        }
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            elapsed_ms = step8.elapsed().as_millis() as u64,
            "Step 7-8: API socket ready"
        );

        // 9. Configure VM via API and start the instance
        let step9 = Instant::now();
        let dev_path_str = snapshot.device_path.to_string_lossy().into_owned();
        let kernel_str = self.kernel_image_path.to_string_lossy().into_owned();

        let configure_result = async {
            api_client
                .configure_boot_source(&kernel_str, &boot_args)
                .await
                .context("Failed to configure boot source")?;
            api_client
                .configure_rootfs(&dev_path_str)
                .await
                .context("Failed to configure rootfs drive")?;
            api_client
                .configure_machine(vcpus, memory_mib)
                .await
                .context("Failed to configure machine")?;
            api_client
                .configure_network(&cni_result.tap_device, &cni_result.guest_mac)
                .await
                .context("Failed to configure network interface")?;
            api_client
                .start_instance()
                .await
                .context("Failed to start VM instance")?;

            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(e) = configure_result {
            log_cancel.cancel();
            let mut child = child;
            let _ = child.kill().await;
            self.cni.teardown_network(&vm_id).await;
            let _ =
                dm_thin::destroy_snapshot_async(snapshot.lv_name.clone(), self.lvm_config.clone())
                    .await;
            let _ = std::fs::remove_file(&socket_path);
            return Err(e);
        }
        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            elapsed_ms = step9.elapsed().as_millis() as u64,
            total_ms = start_time.elapsed().as_millis() as u64,
            "Step 9: VM configured and started"
        );

        // 10. Build addresses
        let (daemon_addr, http_addr) = Self::build_daemon_addrs(&cni_result.guest_ip);

        // 11. Persist metadata for recovery
        let metadata = VmMetadata {
            handle_id: handle_id.clone(),
            vm_id: vm_id.clone(),
            pid: fc_pid,
            lv_name: snapshot.lv_name.clone(),
            netns_name: cni_result.netns_name,
            guest_ip: cni_result.guest_ip.clone(),
            daemon_addr: daemon_addr.clone(),
            http_addr: http_addr.clone(),
            socket_path: socket_path.clone(),
            labels,
            is_warm: false,
            max_memory_mib: None,
            max_vcpus: None,
            dm_device_name: None,
            empty_lv_name: None,
            cgroup_path: None,
        };
        metadata.save(&self.state_dir)?;

        // 12. Insert into in-memory registry
        self.vms.lock().await.insert(
            handle_id.clone(),
            VmState {
                process: VmProcess::Owned(child),
                metadata,
                log_cancel: Some(log_cancel),
            },
        );

        tracing::info!(
            vm_id = %vm_id,
            pool = %pool,
            namespace = %namespace,
            container_id = %container_id,
            sandbox_id = %sandbox_id,
            daemon_addr = %daemon_addr,
            total_ms = start_time.elapsed().as_millis() as u64,
            is_restore,
            "VM creation complete"
        );

        Ok(ProcessHandle {
            id: handle_id,
            daemon_addr: Some(daemon_addr),
            http_addr: Some(http_addr),
            container_ip: cni_result.guest_ip,
        })
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        // Extract the info needed to check liveness, then drop the lock
        // before doing any filesystem I/O (/proc check for Recovered VMs).
        let check = {
            let mut vms = self.vms.lock().await;
            match vms.get_mut(&handle.id) {
                Some(vm) => match &mut vm.process {
                    VmProcess::Owned(child) => {
                        // try_wait is non-blocking, safe under lock
                        Some(matches!(child.try_wait(), Ok(None)))
                    }
                    VmProcess::Recovered { pid } => {
                        // Need to check /proc — just grab the PID
                        let pid = *pid;
                        drop(vms);
                        return Ok(std::path::Path::new(&format!("/proc/{}", pid)).exists());
                    }
                },
                None => Some(false),
            }
        };
        Ok(check.unwrap_or(false))
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        let (vm_id, lv_name, socket_path, dm_device_name, empty_lv_name) = {
            let mut vms = self.vms.lock().await;
            if let Some(vm) = vms.get_mut(&handle.id) {
                // Kill the Firecracker process
                if let Err(e) = vm.process.kill() {
                    tracing::warn!(
                        handle_id = %handle.id,
                        error = ?e,
                        "Failed to kill Firecracker process"
                    );
                }
                // Wait for the process to actually exit before releasing
                // its thin LV. Without this, `lvremove` may fail because
                // the device is still in use.
                vm.process.wait_for_exit().await;
                (
                    vm.metadata.vm_id.clone(),
                    vm.metadata.lv_name.clone(),
                    vm.metadata.socket_path.clone(),
                    vm.metadata.dm_device_name.clone(),
                    vm.metadata.empty_lv_name.clone(),
                )
            } else {
                return Ok(());
            }
        };

        // Track whether this was a warm-pool-managed VM (has dm device).
        let was_warm_pool_vm = dm_device_name.is_some();

        // Clean up dm device if this was a warm pool VM
        if let Some(dm_name) = &dm_device_name {
            dm_swap::remove_dm_device_async(dm_name.clone()).await;
        }

        // Clean up empty LV if present
        if let Some(empty_lv) = &empty_lv_name {
            let _ =
                dm_thin::destroy_snapshot_async(empty_lv.clone(), self.lvm_config.clone()).await;
        }

        // Clean up all resources
        self.cleanup_vm(&handle.id, &vm_id, &lv_name, &socket_path)
            .await;

        // Only notify warm pool for VMs that were tracked in total_vms
        // (warm-pool-booted VMs). Cold-booted VMs were never counted, so
        // decrementing would corrupt the counter.
        if was_warm_pool_vm && let Some(warm_pool) = &self.warm_pool {
            let mut pool = warm_pool.lock().await;
            pool.notify_destroyed();
        }

        Ok(())
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        let vms = self.vms.lock().await;
        if let Some(vm) = vms.get(&handle.id) {
            vm.process.send_signal(signal)?;
        }
        Ok(())
    }

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        let mut vms = self.vms.lock().await;
        if let Some(vm) = vms.get_mut(&handle.id) {
            if let Some(status) = vm.process.try_exit_status() {
                let exit_code = status.code().map(|c| c as i64);
                // SIGKILL (exit code 137 = 128+9) is sent by the Linux OOM killer.
                let oom_killed = exit_code == Some(137);
                Ok(Some(ExitStatus {
                    exit_code,
                    oom_killed,
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    async fn list_containers(&self) -> Result<Vec<String>> {
        let vms = self.vms.lock().await;
        Ok(vms.keys().cloned().collect())
    }

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
        let vm_id = handle
            .id
            .strip_prefix("fc-")
            .unwrap_or(&handle.id)
            .to_string();
        let log_path = self.log_dir.join(format!("fc-{}.log", vm_id));

        tokio::task::spawn_blocking(move || {
            use std::io::{Read, Seek, SeekFrom};

            if !log_path.exists() {
                return Ok(String::new());
            }

            if tail == 0 {
                return std::fs::read_to_string(&log_path)
                    .with_context(|| format!("Failed to read log file {}", log_path.display()));
            }

            // Read from the end to find the last `tail` lines efficiently.
            let mut file = std::fs::File::open(&log_path)
                .with_context(|| format!("Failed to open log file {}", log_path.display()))?;
            let file_len = file.metadata()?.len();
            // Read at most 64KB from the tail — enough for most log tails.
            let read_size = file_len.min(64 * 1024) as usize;
            file.seek(SeekFrom::End(-(read_size as i64)))?;
            let mut buf = vec![0u8; read_size];
            file.read_exact(&mut buf)?;
            let content = String::from_utf8_lossy(&buf);

            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(tail as usize);
            Ok(lines[start..].join("\n"))
        })
        .await
        .context("get_logs task panicked")?
    }
}
