//! Firecracker microVM driver for Indexify dataplane.
//!
//! Provides hardware-virtualized isolation using Firecracker microVMs with
//! dm-snapshot CoW volumes for rootfs and CNI networking.
//!
//! Uses dm-snapshot (not dm-thin) so each VM's COW data lives in a separate
//! file on disk, making snapshot/restore trivial (the COW file IS the delta).

pub(crate) mod api;
mod cni;
pub(crate) mod dm_snapshot;
mod log_stream;
mod rootfs;
pub(crate) mod vm_state;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use dm_snapshot::ThinDeviceAllocator;
use tokio::sync::Mutex;

use self::{
    api::FirecrackerApiClient,
    cni::CniManager,
    dm_snapshot::OriginHandle,
    vm_state::{
        OriginMetadata,
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

/// Firecracker microVM driver implementing the `ProcessDriver` trait.
pub struct FirecrackerDriver {
    /// Path to the firecracker binary.
    firecracker_binary: String,
    /// Path to the Linux kernel image.
    kernel_image_path: PathBuf,
    /// Origin (base rootfs) device handle.
    origin: OriginHandle,
    /// LVM thin pool configuration for per-VM COW devices.
    lvm_config: dm_snapshot::LvmConfig,
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
    /// Per-VM COW LV size in bytes.
    default_rootfs_size_bytes: u64,
    /// Directory for API sockets and VM metadata.
    state_dir: PathBuf,
    /// Directory for VM log files.
    log_dir: PathBuf,
    /// Thin device ID allocator for COW devices.
    allocator: Arc<ThinDeviceAllocator>,
    /// In-memory registry of running VMs.
    vms: Arc<Mutex<HashMap<String, VmState>>>,
}

impl FirecrackerDriver {
    /// Create a new Firecracker driver.
    ///
    /// Validates paths, creates directories, sets up the origin device,
    /// and recovers any VMs from a previous run.
    pub fn new(
        firecracker_binary: Option<String>,
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
    ) -> Result<Self> {
        let firecracker_binary = firecracker_binary.unwrap_or_else(|| "firecracker".to_string());
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

        let mut lvm_config = dm_snapshot::LvmConfig {
            volume_group: lvm_volume_group,
            thin_pool: lvm_thin_pool,
            pool_dm_device: String::new(), // populated by validate_lvm_config
        };
        dm_snapshot::validate_lvm_config(&mut lvm_config)
            .context("LVM thin pool validation failed")?;

        let allocator = Arc::new(ThinDeviceAllocator::new(&state_dir, &lvm_config));

        let cni = CniManager::new(cni_network_name, cni_bin_path);

        // --- Origin device lifecycle ---
        //
        // The origin is a read-only linear dm device backed by the base rootfs
        // file via a loop device. Each per-VM volume is a dm-snapshot of this
        // origin, with CoW data stored in a separate file.
        //
        // If the base image path has changed since last run, tear down the old
        // origin first so setup_origin() creates a fresh one.
        if let Ok(Some(old_meta)) = OriginMetadata::load(&state_dir) &&
            old_meta.base_image_path != base_rootfs_image
        {
            tracing::info!(
                old_image = %old_meta.base_image_path,
                new_image = %base_rootfs_image,
                "Base rootfs image changed, tearing down old origin"
            );
            let old_handle = dm_snapshot::OriginHandle {
                dm_name: old_meta.dm_name,
                loop_device: old_meta.loop_device,
                device_path: PathBuf::from("/dev/mapper/indexify-base"),
                size_sectors: 0, // not needed for teardown
            };
            if let Err(e) = dm_snapshot::teardown_origin(&old_handle) {
                tracing::warn!(error = ?e, "Failed to tear down old origin (continuing)");
            }
            OriginMetadata::remove(&state_dir);
        }

        let origin =
            dm_snapshot::setup_origin(&rootfs_path).context("Failed to set up origin device")?;

        // Persist origin metadata for recovery.
        let origin_meta = OriginMetadata {
            base_image_path: base_rootfs_image.clone(),
            loop_device: origin.loop_device.clone(),
            dm_name: origin.dm_name.clone(),
        };
        origin_meta.save(&state_dir)?;

        // Recover VMs from metadata files.
        let metadata_list = scan_metadata_files(&state_dir)?;
        let mut recovered_vms = HashMap::new();
        let mut dead_vms = Vec::new();

        for metadata in metadata_list {
            if is_firecracker_process(metadata.pid) {
                tracing::info!(
                    vm_id = %metadata.vm_id,
                    pid = metadata.pid,
                    dm_name = %metadata.dm_name,
                    "Recovered running Firecracker VM"
                );

                // dm-snapshot targets persist in the kernel across dataplane
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

        // Clean up dead VMs (those with metadata but whose process is gone).
        let driver = Self {
            firecracker_binary,
            kernel_image_path: kernel_path,
            origin,
            lvm_config,
            cni,
            guest_gateway,
            guest_netmask,
            default_vcpu_count: vcpu_count,
            default_memory_mib: memory_mib,
            default_rootfs_size_bytes: rootfs_size,
            state_dir,
            log_dir,
            allocator,
            vms: Arc::new(Mutex::new(recovered_vms)),
        };

        for metadata in &dead_vms {
            driver.cleanup_dead_vm(metadata);
        }

        // Clean up leaked dm-snapshot devices from crashed VMs that lost
        // their metadata files (metadata gone, but kernel devices remain).
        dm_snapshot::cleanup_stale_devices(&active_vm_ids, &driver.lvm_config);

        // Clean up leaked network namespaces from crashed VMs.
        driver.cni.cleanup_orphaned_netns_sync(&active_vm_ids);

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
        let _ = std::fs::remove_file(
            self.log_dir
                .join(format!("fc-{}-serial.log", metadata.vm_id)),
        );

        // dm-snapshot and thin device cleanup happen asynchronously.
        let dm_name = metadata.dm_name.clone();
        let thin_device_id = metadata.thin_device_id;
        let vm_id = metadata.vm_id.clone();
        let lvm_config = self.lvm_config.clone();
        let cni_network_name = self.cni.network_name().to_string();
        let cni_bin_path = self.cni.cni_bin_path().to_string();

        tokio::spawn(async move {
            if let Err(e) =
                dm_snapshot::destroy_snapshot_by_parts_async(dm_name, lvm_config, thin_device_id)
                    .await
            {
                tracing::warn!(
                    error = ?e,
                    "Failed to destroy dm-snapshot for dead VM"
                );
            }

            // Teardown CNI
            let cni = CniManager::new(cni_network_name, cni_bin_path);
            cni.teardown_network(&vm_id).await;
        });
    }

    /// Clean up all resources for a VM after it has been killed.
    async fn cleanup_vm(
        &self,
        handle_id: &str,
        vm_id: &str,
        dm_name: &str,
        thin_device_id: u32,
        socket_path: &str,
    ) {
        // Cancel log streamer and remove from registry.
        {
            let mut vms = self.vms.lock().await;
            if let Some(vm) = vms.remove(handle_id) {
                if let Some(cancel) = &vm.log_cancel {
                    cancel.cancel();
                }
            }
        }

        // Destroy dm-snapshot and remove COW thin device.
        if let Err(e) = dm_snapshot::destroy_snapshot_by_parts_async(
            dm_name.to_string(),
            self.lvm_config.clone(),
            thin_device_id,
        )
        .await
        {
            tracing::warn!(dm_name, error = ?e, "Failed to destroy dm-snapshot");
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

        // 1. Create dm-snapshot for this VM. Check if config.image points to a .cow
        //    file (restore path).
        let snapshot = if let Some(ref image) = config.image {
            if image.ends_with(".cow") {
                // Restore path: use existing COW file from snapshotter.
                dm_snapshot::create_snapshot_from_cow_async(
                    self.origin.dm_name.clone(),
                    self.origin.device_path.clone(),
                    self.origin.size_sectors,
                    vm_id.clone(),
                    self.lvm_config.clone(),
                    PathBuf::from(image),
                    self.allocator.clone(),
                )
                .await
                .with_context(|| {
                    format!(
                        "Failed to create dm-snapshot from COW file for VM {}",
                        vm_id
                    )
                })?
            } else {
                // Normal path: create fresh thin device COW.
                dm_snapshot::create_snapshot_async(
                    self.origin.dm_name.clone(),
                    self.origin.device_path.clone(),
                    self.origin.size_sectors,
                    vm_id.clone(),
                    self.lvm_config.clone(),
                    self.default_rootfs_size_bytes,
                    self.allocator.clone(),
                )
                .await
                .with_context(|| format!("Failed to create dm-snapshot for VM {}", vm_id))?
            }
        } else {
            // Normal path: create fresh thin device COW.
            dm_snapshot::create_snapshot_async(
                self.origin.dm_name.clone(),
                self.origin.device_path.clone(),
                self.origin.size_sectors,
                vm_id.clone(),
                self.lvm_config.clone(),
                self.default_rootfs_size_bytes,
                self.allocator.clone(),
            )
            .await
            .with_context(|| format!("Failed to create dm-snapshot for VM {}", vm_id))?
        };

        // 2. Get the daemon binary path
        let daemon_binary =
            crate::daemon_binary::get_daemon_path().context("Daemon binary not available")?;
        tracing::info!(vm_id = %vm_id, daemon_path = %daemon_binary.display(), "Injecting rootfs");

        // 3. Inject daemon, init script, and env vars into the snapshot. Include the
        //    host's DNS nameservers so the VM can resolve names.
        let mut env_vars = config.env.clone();
        if let Some(dns) = rootfs::read_host_dns() {
            env_vars.push(("DNS_NAMESERVERS".to_string(), dns));
        }
        if let Err(e) =
            rootfs::inject_rootfs(&snapshot.device_path, daemon_binary, &env_vars, &vm_id).await
        {
            let _ = dm_snapshot::destroy_snapshot_async(snapshot, self.lvm_config.clone()).await;
            return Err(e.context("Failed to inject rootfs"));
        }
        tracing::info!(vm_id = %vm_id, "Rootfs injection complete, setting up CNI");

        // 4. Setup CNI networking
        let cni_result = match self.cni.setup_network(&vm_id).await {
            Ok(result) => result,
            Err(e) => {
                let _ =
                    dm_snapshot::destroy_snapshot_async(snapshot, self.lvm_config.clone()).await;
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

        tracing::info!(vm_id = %vm_id, guest_ip = %cni_result.guest_ip, "CNI setup complete, spawning Firecracker");

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
                let _ =
                    dm_snapshot::destroy_snapshot_async(snapshot, self.lvm_config.clone()).await;
                return Err(anyhow::anyhow!("Failed to spawn firecracker: {}", e));
            }
        };

        let fc_pid = child.id().unwrap_or(0);

        // 8. Spawn log streamer
        let labels: HashMap<String, String> = config.labels.into_iter().collect();
        let log_cancel =
            log_stream::spawn_log_streamer(vm_id.clone(), self.log_dir.clone(), labels.clone());

        // 9. Wait for API socket
        let api_client = FirecrackerApiClient::new(&socket_path);
        if let Err(e) = api_client
            .wait_for_socket(API_SOCKET_TIMEOUT, API_SOCKET_POLL_INTERVAL)
            .await
        {
            log_cancel.cancel();
            let mut child = child;
            let _ = child.kill().await;
            self.cni.teardown_network(&vm_id).await;
            let _ = dm_snapshot::destroy_snapshot_async(snapshot, self.lvm_config.clone()).await;
            let _ = std::fs::remove_file(&socket_path);
            return Err(e.context("Firecracker API socket not ready"));
        }

        // 10. Configure VM via API
        let dev_path_str = snapshot.device_path.to_string_lossy().to_string();
        let kernel_str = self.kernel_image_path.to_string_lossy().to_string();

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
            let _ = dm_snapshot::destroy_snapshot_async(snapshot, self.lvm_config.clone()).await;
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
            thin_device_id: snapshot.thin_device_id,
            dm_name: snapshot.dm_name.clone(),
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
        let (vm_id, dm_name, thin_device_id, socket_path) = {
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
                // its dm-snapshot device.  Without this, `dmsetup remove`
                // fails with "Device or resource busy".
                vm.process.wait_for_exit().await;
                (
                    vm.metadata.vm_id.clone(),
                    vm.metadata.dm_name.clone(),
                    vm.metadata.thin_device_id,
                    vm.metadata.socket_path.clone(),
                )
            } else {
                return Ok(());
            }
        };

        // Clean up all resources
        self.cleanup_vm(&handle.id, &vm_id, &dm_name, thin_device_id, &socket_path)
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
        let metadata_list = scan_metadata_files(&self.state_dir)?;
        Ok(metadata_list.into_iter().map(|m| m.handle_id).collect())
    }

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
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

            let lines: Vec<&str> = content.lines().collect();
            let start = lines.len().saturating_sub(tail as usize);
            Ok(lines[start..].join("\n"))
        })
        .await
        .context("get_logs task panicked")?
    }
}
