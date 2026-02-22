//! Firecracker microVM driver for Indexify dataplane.
//!
//! Provides hardware-virtualized isolation using Firecracker microVMs with
//! device-mapper thin provisioning for rootfs and CNI networking.
//!
//! This is Phase 1: core VM lifecycle (start, stop, kill, health checks,
//! recovery). Phase 2 adds snapshot/restore via dm-thin snapshots +
//! Firecracker memory snapshots. Phase 3 adds S3 archival.

mod api;
mod cni;
mod log_stream;
mod rootfs;
mod thin_pool;
mod vm_state;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use tokio::sync::Mutex;

use self::api::FirecrackerApiClient;
use self::cni::CniManager;
use self::thin_pool::ThinPoolManager;
use self::vm_state::{
    OriginMetadata, VmMetadata, VmProcess, VmState, is_firecracker_process, scan_metadata_files,
    sha256_file,
};
use super::{
    DAEMON_GRPC_PORT, DAEMON_HTTP_PORT, ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle,
};

/// Default thin pool block size in sectors (128 sectors = 64KB).
const DEFAULT_BLOCK_SIZE: u64 = 128;
/// Default thin pool low water mark in data blocks.
const DEFAULT_LOW_WATER_MARK: u64 = 1024;
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
/// Thin device ID reserved for the origin volume (base rootfs image).
/// All per-VM volumes are CoW snapshots of this origin.
const ORIGIN_THIN_ID: u32 = 0;
/// Timeout for waiting for Firecracker API socket.
const API_SOCKET_TIMEOUT: Duration = Duration::from_secs(5);
/// Poll interval for API socket.
const API_SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Firecracker microVM driver implementing the `ProcessDriver` trait.
pub struct FirecrackerDriver {
    /// Path to the firecracker binary.
    firecracker_binary: String,
    /// Path to the Linux kernel image.
    kernel_image_path: PathBuf,
    /// Thin pool manager (device-mapper).
    thin_pool: Arc<Mutex<ThinPoolManager>>,
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
    /// Per-VM thin volume size in bytes.
    default_rootfs_size_bytes: u64,
    /// Directory for API sockets and VM metadata.
    state_dir: PathBuf,
    /// Directory for VM log files.
    log_dir: PathBuf,
    /// In-memory registry of running VMs.
    vms: Arc<Mutex<HashMap<String, VmState>>>,
    /// Monotonically increasing thin device ID allocator.
    next_thin_id: AtomicU32,
}

impl FirecrackerDriver {
    /// Create a new Firecracker driver.
    ///
    /// Validates paths, creates directories, initializes the thin pool,
    /// and recovers any VMs from a previous run.
    pub fn new(
        firecracker_binary: Option<String>,
        kernel_image_path: String,
        thin_pool_meta_device: String,
        thin_pool_data_device: String,
        thin_pool_block_size: Option<u64>,
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
    ) -> Result<Self> {
        let firecracker_binary = firecracker_binary.unwrap_or_else(|| "firecracker".to_string());
        let kernel_path = PathBuf::from(&kernel_image_path);
        let rootfs_path = PathBuf::from(&base_rootfs_image);
        let cni_bin_path = cni_bin_path.unwrap_or_else(|| DEFAULT_CNI_BIN_PATH.to_string());
        let guest_netmask = guest_netmask.unwrap_or_else(|| DEFAULT_GUEST_NETMASK.to_string());
        let block_size = thin_pool_block_size.unwrap_or(DEFAULT_BLOCK_SIZE);
        let rootfs_size = default_rootfs_size_bytes.unwrap_or(DEFAULT_ROOTFS_SIZE_BYTES);
        let vcpu_count = default_vcpu_count.unwrap_or(DEFAULT_VCPU_COUNT);
        let memory_mib = default_memory_mib.unwrap_or(DEFAULT_MEMORY_MIB);

        // Validate paths
        if !kernel_path.exists() {
            bail!(
                "Kernel image not found: {}",
                kernel_path.display()
            );
        }
        if !rootfs_path.exists() {
            bail!(
                "Base rootfs image not found: {}",
                rootfs_path.display()
            );
        }

        // Create directories
        std::fs::create_dir_all(&state_dir)
            .with_context(|| format!("Failed to create state dir {}", state_dir.display()))?;
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("Failed to create log dir {}", log_dir.display()))?;

        // Initialize thin pool
        let mut thin_pool = ThinPoolManager::new(
            &thin_pool_meta_device,
            &thin_pool_data_device,
            block_size,
            DEFAULT_LOW_WATER_MARK,
        )
        .context("Failed to initialize thin pool")?;

        let cni = CniManager::new(cni_network_name, cni_bin_path);

        // Recover VMs from metadata files before constructing the driver.
        // This avoids needing to lock the vms Mutex (which would panic if
        // called from within a tokio runtime via blocking_lock).
        let metadata_list = scan_metadata_files(&state_dir)?;
        let mut recovered_vms = HashMap::new();
        let mut max_thin_id: u32 = 0;
        let mut dead_vms = Vec::new();

        // Calculate rootfs size in sectors for reconnecting thin volumes.
        let rootfs_size_sectors = rootfs_size / 512;

        for metadata in metadata_list {
            max_thin_id = max_thin_id.max(metadata.thin_id);

            if is_firecracker_process(metadata.pid) {
                tracing::info!(
                    vm_id = %metadata.vm_id,
                    pid = metadata.pid,
                    thin_id = metadata.thin_id,
                    "Recovered running Firecracker VM"
                );

                // Reconnect to the thin device so destroy_volume() works later.
                if let Err(e) = thin_pool.reconnect_volume(metadata.thin_id, rootfs_size_sectors) {
                    tracing::warn!(
                        thin_id = metadata.thin_id,
                        error = ?e,
                        "Failed to reconnect thin volume for recovered VM"
                    );
                }

                // Spawn log streamer for recovered VM (seeks to end, only
                // streams new lines written after this restart).
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

        // --- Origin volume lifecycle (Phase 2a: CoW snapshots) ---
        //
        // Reserve thin_id=0 for the origin volume. This volume holds the base
        // rootfs image and is snapshotted (CoW) for each per-VM volume instead
        // of doing a full `dd` copy.
        let base_image_hash = sha256_file(&rootfs_path)
            .context("Failed to hash base rootfs image")?;

        let origin_meta = OriginMetadata::load(&state_dir)?;
        let origin_valid = origin_meta
            .as_ref()
            .map(|m| {
                m.base_image_hash == base_image_hash
                    && std::path::Path::new("/dev/mapper/indexify-thin-0").exists()
            })
            .unwrap_or(false);

        if origin_valid {
            // Origin exists and matches current base image — reconnect.
            if let Err(e) = thin_pool.reconnect_volume(ORIGIN_THIN_ID, rootfs_size_sectors) {
                tracing::warn!(
                    error = ?e,
                    "Failed to reconnect origin volume, will recreate"
                );
                // Fall through to recreation below
                Self::recreate_origin(
                    &mut thin_pool,
                    &rootfs_path,
                    rootfs_size_sectors,
                    &base_image_hash,
                    &state_dir,
                )?;
            } else {
                tracing::info!("Reconnected to existing origin volume (thin_id=0)");
            }
        } else {
            // First run, or base image changed — create (or recreate) origin.
            if let Some(ref meta) = origin_meta {
                tracing::info!(
                    old_hash = %meta.base_image_hash,
                    new_hash = %base_image_hash,
                    "Base rootfs image changed, recreating origin volume"
                );
            } else {
                tracing::info!("No origin volume found, creating from base rootfs image");
            }

            // If the stale origin device exists in kernel, destroy it first.
            if std::path::Path::new("/dev/mapper/indexify-thin-0").exists() {
                if let Err(e) = thin_pool.destroy_volume(ORIGIN_THIN_ID) {
                    tracing::warn!(error = ?e, "Failed to destroy stale origin volume");
                }
            }

            Self::recreate_origin(
                &mut thin_pool,
                &rootfs_path,
                rootfs_size_sectors,
                &base_image_hash,
                &state_dir,
            )?;
        }

        let initial_thin_id = if max_thin_id > 0 { max_thin_id + 1 } else { 1 };

        let driver = Self {
            firecracker_binary,
            kernel_image_path: kernel_path,
            thin_pool: Arc::new(Mutex::new(thin_pool)),
            cni,
            guest_gateway,
            guest_netmask,
            default_vcpu_count: vcpu_count,
            default_memory_mib: memory_mib,
            default_rootfs_size_bytes: rootfs_size,
            state_dir,
            log_dir,
            vms: Arc::new(Mutex::new(recovered_vms)),
            next_thin_id: AtomicU32::new(initial_thin_id),
        };

        // Clean up dead VMs (file removal is sync, thin/netns cleanup spawns async tasks)
        for metadata in &dead_vms {
            driver.cleanup_dead_vm(metadata);
        }

        Ok(driver)
    }

    /// Clean up resources for a dead VM (blocking, used during recovery).
    fn cleanup_dead_vm(&self, metadata: &VmMetadata) {
        // Remove metadata file
        metadata.remove(&self.state_dir);

        // Remove socket file
        let _ = std::fs::remove_file(&metadata.socket_path);

        // Remove log files
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}.log", metadata.vm_id)));
        let _ =
            std::fs::remove_file(self.log_dir.join(format!("fc-{}-serial.log", metadata.vm_id)));

        // Thin volume and netns cleanup happen asynchronously in background
        // since they may require privileges. We log warnings on failure.
        let thin_id = metadata.thin_id;
        let vm_id = metadata.vm_id.clone();
        let thin_pool = self.thin_pool.clone();
        let cni_network_name = self.cni.network_name().to_string();
        let cni_bin_path = self.cni.cni_bin_path().to_string();

        tokio::spawn(async move {
            // Guard: never destroy the origin volume during VM cleanup.
            if thin_id == ORIGIN_THIN_ID {
                tracing::warn!("Refusing to destroy origin volume (thin_id=0) during dead VM cleanup");
            } else if let Err(e) = thin_pool.lock().await.destroy_volume(thin_id) {
                tracing::warn!(
                    thin_id,
                    error = ?e,
                    "Failed to destroy thin volume for dead VM"
                );
            }

            // Teardown CNI
            let cni = CniManager::new(cni_network_name, cni_bin_path);
            cni.teardown_network(&vm_id).await;
        });
    }

    /// Create (or recreate) the origin volume from the base rootfs image.
    ///
    /// This is a one-time cost (~3-5s for a 1 GiB image) that only runs on
    /// first startup or when the base image changes.
    fn recreate_origin(
        thin_pool: &mut ThinPoolManager,
        base_rootfs_image: &Path,
        rootfs_size_sectors: u64,
        base_image_hash: &str,
        state_dir: &Path,
    ) -> Result<()> {
        // Try to create the origin volume. If it already exists in the pool
        // metadata (e.g., from a previous run with a different state_dir),
        // reconnect to it, destroy it, and try again.
        let origin_dev_path = match thin_pool.create_volume(ORIGIN_THIN_ID, rootfs_size_sectors) {
            Ok(path) => path,
            Err(_) => {
                tracing::info!(
                    "Origin thin_id=0 already exists in pool metadata, \
                     destroying and recreating"
                );
                // Reconnect so we can destroy it properly.
                thin_pool
                    .reconnect_volume(ORIGIN_THIN_ID, rootfs_size_sectors)
                    .context("Failed to reconnect to stale origin volume")?;
                thin_pool
                    .destroy_volume(ORIGIN_THIN_ID)
                    .context("Failed to destroy stale origin volume")?;
                thin_pool
                    .create_volume(ORIGIN_THIN_ID, rootfs_size_sectors)
                    .context("Failed to create origin thin volume after cleanup")?
            }
        };

        // dd the base image onto the origin — blocking, one-time cost.
        tracing::info!(
            origin_dev = %origin_dev_path.display(),
            "Populating origin volume from base rootfs image (one-time cost)"
        );

        // Use std::process::Command (blocking) since we're in a sync context.
        let output = std::process::Command::new("dd")
            .args([
                &format!("if={}", base_rootfs_image.display()),
                &format!("of={}", origin_dev_path.display()),
                "bs=4M",
                "conv=fsync",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .context("Failed to execute dd for origin volume")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Destroy the origin volume on failure so we retry next time.
            let _ = thin_pool.destroy_volume(ORIGIN_THIN_ID);
            anyhow::bail!(
                "dd failed populating origin volume: {}",
                stderr
            );
        }

        // Persist origin metadata so we can reconnect on next startup.
        let origin_metadata = OriginMetadata {
            base_image_hash: base_image_hash.to_string(),
            thin_id: ORIGIN_THIN_ID,
        };
        origin_metadata.save(state_dir)?;
        tracing::info!("Origin volume created and metadata saved");

        Ok(())
    }

    /// Allocate the next thin device ID.
    fn alloc_thin_id(&self) -> u32 {
        self.next_thin_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Clean up all resources for a VM after it has been killed.
    async fn cleanup_vm(
        &self,
        handle_id: &str,
        vm_id: &str,
        thin_id: u32,
        socket_path: &str,
    ) {
        // Cancel log streamer and remove from registry in a single lock scope
        // to avoid a window where another task sees an inconsistent state.
        {
            let mut vms = self.vms.lock().await;
            if let Some(vm) = vms.remove(handle_id) {
                if let Some(cancel) = &vm.log_cancel {
                    cancel.cancel();
                }
            }
        }

        // Guard: never destroy the origin volume during VM cleanup.
        if thin_id == ORIGIN_THIN_ID {
            tracing::warn!("Refusing to destroy origin volume (thin_id=0) during VM cleanup");
        } else if let Err(e) = self.thin_pool.lock().await.destroy_volume(thin_id) {
            tracing::warn!(thin_id, error = ?e, "Failed to destroy thin volume");
        }

        // Teardown CNI networking
        self.cni.teardown_network(vm_id).await;

        // Remove socket file
        let _ = std::fs::remove_file(socket_path);

        // Remove metadata file
        let metadata_path = self.state_dir.join(format!("fc-{}.json", vm_id));
        let _ = std::fs::remove_file(metadata_path);

        // Remove log files
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}.log", vm_id)));
        let _ = std::fs::remove_file(self.log_dir.join(format!("fc-{}-serial.log", vm_id)));
    }
}

#[async_trait]
impl ProcessDriver for FirecrackerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let vm_id = config.id.clone();
        let handle_id = format!("fc-{}", vm_id);
        let thin_id = self.alloc_thin_id();

        // Calculate rootfs size in sectors (512 bytes each)
        let rootfs_size_sectors = self.default_rootfs_size_bytes / 512;

        // 1. Create CoW snapshot of origin volume (<1ms)
        let thin_dev_path = self
            .thin_pool
            .lock()
            .await
            .create_snapshot(ORIGIN_THIN_ID, thin_id, rootfs_size_sectors)
            .with_context(|| format!("Failed to snapshot origin for VM {}", vm_id))?;

        // 2. Get the daemon binary path
        let daemon_binary = crate::daemon_binary::get_daemon_path()
            .context("Daemon binary not available")?;

        // 3. Inject daemon, init script, and env vars into the snapshot.
        //    No dd needed — the snapshot already has the base image via CoW.
        if let Err(e) =
            rootfs::inject_rootfs(&thin_dev_path, daemon_binary, &config.env, &vm_id).await
        {
            let _ = self.thin_pool.lock().await.destroy_volume(thin_id);
            return Err(e.context("Failed to inject rootfs"));
        }

        // 4. Setup CNI networking
        let cni_result = match self.cni.setup_network(&vm_id).await {
            Ok(result) => result,
            Err(e) => {
                let _ = self.thin_pool.lock().await.destroy_volume(thin_id);
                return Err(e.context("Failed to setup CNI networking"));
            }
        };

        // 5. Compute resource limits
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

        // 6. Build kernel boot args
        let boot_args = format!(
            "console=ttyS0 reboot=k panic=1 pci=off \
             ip={}::{}:{}::eth0:off \
             init=/sbin/indexify-init",
            cni_result.guest_ip, self.guest_gateway, self.guest_netmask,
        );

        // 7. Spawn Firecracker process
        let socket_path = self
            .state_dir
            .join(format!("fc-{}.sock", vm_id))
            .to_string_lossy()
            .to_string();
        let log_path = self
            .log_dir
            .join(format!("fc-{}.log", vm_id))
            .to_string_lossy()
            .to_string();

        // Remove stale socket if present
        let _ = std::fs::remove_file(&socket_path);

        // Run Firecracker inside the network namespace so it can see the
        // TAP device created by the CNI tc-redirect-tap plugin.
        // Write serial console output to a file for debugging.
        let serial_path = self
            .log_dir
            .join(format!("fc-{}-serial.log", vm_id));
        let serial_file = std::fs::File::create(&serial_path).or_else(|_| {
            std::fs::File::create("/dev/null")
        }).with_context(|| format!(
            "Failed to create serial log file {} or /dev/null fallback",
            serial_path.display()
        ))?;

        let child = match tokio::process::Command::new("ip")
            .args([
                "netns", "exec", &cni_result.netns_name,
                &self.firecracker_binary,
                "--api-sock", &socket_path,
                "--log-path", &log_path,
            ])
            .stdout(serial_file)
            .stderr(Stdio::null())
            .kill_on_drop(false)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                self.cni.teardown_network(&vm_id).await;
                let _ = self.thin_pool.lock().await.destroy_volume(thin_id);
                return Err(anyhow::anyhow!(
                    "Failed to spawn firecracker: {}",
                    e
                ));
            }
        };

        let fc_pid = child.id().unwrap_or(0);

        // 8. Spawn log streamer early so it captures boot-time output
        //    (VMM log + serial console). Cancel it on any error path below.
        let labels: HashMap<String, String> = config.labels.into_iter().collect();
        let log_cancel = log_stream::spawn_log_streamer(
            vm_id.clone(),
            self.log_dir.clone(),
            labels.clone(),
        );

        // 9. Wait for API socket
        let api_client = FirecrackerApiClient::new(&socket_path);
        if let Err(e) = api_client
            .wait_for_socket(API_SOCKET_TIMEOUT, API_SOCKET_POLL_INTERVAL)
            .await
        {
            log_cancel.cancel();
            // Kill the process we just spawned
            let mut child = child;
            let _ = child.kill().await;
            self.cni.teardown_network(&vm_id).await;
            let _ = self.thin_pool.lock().await.destroy_volume(thin_id);
            let _ = std::fs::remove_file(&socket_path);
            return Err(e.context("Firecracker API socket not ready"));
        }

        // 10. Configure VM via API
        let thin_dev_str = thin_dev_path.to_string_lossy().to_string();
        let kernel_str = self.kernel_image_path.to_string_lossy().to_string();

        let configure_result = async {
            api_client
                .configure_boot_source(&kernel_str, &boot_args)
                .await
                .context("Failed to configure boot source")?;
            api_client
                .configure_rootfs(&thin_dev_str)
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

            // 11. Start the instance
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
            let _ = self.thin_pool.lock().await.destroy_volume(thin_id);
            let _ = std::fs::remove_file(&socket_path);
            return Err(e);
        }

        // 12. Build addresses
        let daemon_addr = format!("{}:{}", cni_result.guest_ip, DAEMON_GRPC_PORT);
        let http_addr = format!("{}:{}", cni_result.guest_ip, DAEMON_HTTP_PORT);

        // 13. Persist metadata for recovery
        let metadata = VmMetadata {
            handle_id: handle_id.clone(),
            vm_id: vm_id.clone(),
            pid: fc_pid,
            thin_id,
            netns_name: cni_result.netns_name,
            guest_ip: cni_result.guest_ip.clone(),
            daemon_addr: daemon_addr.clone(),
            http_addr: http_addr.clone(),
            socket_path: socket_path.clone(),
            labels,
        };
        metadata.save(&self.state_dir)?;

        // 14. Insert into in-memory registry
        self.vms.lock().await.insert(
            handle_id.clone(),
            VmState {
                process: VmProcess::Owned(child),
                metadata,
                log_cancel: Some(log_cancel),
            },
        );

        Ok(ProcessHandle {
            id: handle_id,
            daemon_addr: Some(daemon_addr),
            http_addr: Some(http_addr),
            container_ip: cni_result.guest_ip,
        })
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        let mut vms = self.vms.lock().await;
        if let Some(vm) = vms.get_mut(&handle.id) {
            Ok(vm.process.is_alive())
        } else {
            Ok(false)
        }
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        let (vm_id, thin_id, socket_path) = {
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
                (
                    vm.metadata.vm_id.clone(),
                    vm.metadata.thin_id,
                    vm.metadata.socket_path.clone(),
                )
            } else {
                return Ok(());
            }
        };

        // Clean up all resources
        self.cleanup_vm(&handle.id, &vm_id, thin_id, &socket_path)
            .await;

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
                // Exit code 134 typically indicates SIGABRT, which can be OOM
                let oom_killed = exit_code == Some(134);
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
        // Scan state_dir for fc-*.json files to discover containers
        // (includes both in-memory and on-disk state for robustness)
        let metadata_list = scan_metadata_files(&self.state_dir)?;
        Ok(metadata_list
            .into_iter()
            .map(|m| m.handle_id)
            .collect())
    }

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
        // Extract the vm_id from the handle_id (strip "fc-" prefix)
        let vm_id = handle
            .id
            .strip_prefix("fc-")
            .unwrap_or(&handle.id)
            .to_string();
        let log_path = self.log_dir.join(format!("fc-{}.log", vm_id));

        tokio::task::spawn_blocking(move || {
            if !log_path.exists() {
                return Ok(String::new());
            }

            let content = std::fs::read_to_string(&log_path)
                .with_context(|| format!("Failed to read log file {}", log_path.display()))?;

            if tail == 0 {
                return Ok(content);
            }

            // Return the last N lines
            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(tail as usize);
            Ok(lines[start..].join("\n"))
        })
        .await
        .context("get_logs task panicked")?
    }
}
