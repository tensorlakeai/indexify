//! dm-thin native snapshot volume management for Firecracker rootfs.
//!
//! Uses LVM thin provisioning's native snapshot capability. The base rootfs
//! image is imported into a thin LV once, and each VM gets a thin snapshot
//! of that LV. Thin snapshots can be resized beyond the base image size,
//! giving per-VM disk sizing with COW block sharing.
//!
//! Architecture:
//! ```text
//! Base rootfs file (/opt/firecracker/rootfs.ext4)
//!   → dd into thin LV "indexify-base" (one-time import)
//!
//! Per-VM:
//!   lvcreate --snapshot {vg}/indexify-base --name indexify-vm-{vm_id}
//!   → optional lvresize + resize2fs for larger disks
//!   Device: /dev/{vg}/indexify-vm-{vm_id}
//! ```
//!
//! No loop devices, no `dmsetup create`, no separate snapshot devices.

use std::{
    collections::HashSet,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};

/// Name of the shared base image thin LV.
pub const BASE_LV_NAME: &str = "indexify-base";

// ---------------------------------------------------------------------------
// Delta binary format constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying the delta snapshot format (8 bytes).
const DELTA_MAGIC: [u8; 8] = *b"IXDELTA\x01";
/// Total header size in bytes.
const DELTA_HEADER_SIZE: usize = 32;
/// Block size used for delta comparison and records (4 MB).
const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Write the 32-byte delta header into `w`.
pub fn write_delta_header(w: &mut impl Write, block_size: u32, image_size: u64) -> Result<()> {
    w.write_all(&DELTA_MAGIC)?;
    w.write_all(&block_size.to_le_bytes())?;
    w.write_all(&image_size.to_le_bytes())?;
    w.write_all(&[0u8; 12])?; // reserved
    Ok(())
}

/// Read and validate the 32-byte delta header from `r`.
/// Returns `(block_size, image_size)`.
pub fn read_delta_header(r: &mut impl Read) -> Result<(u32, u64)> {
    let mut header = [0u8; DELTA_HEADER_SIZE];
    r.read_exact(&mut header)
        .context("Failed to read delta header")?;

    if header[..8] != DELTA_MAGIC {
        bail!("Invalid delta header: bad magic");
    }

    let block_size = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let image_size = u64::from_le_bytes(header[12..20].try_into().unwrap());
    Ok((block_size, image_size))
}

/// LVM thin pool configuration for per-VM thin snapshots.
#[derive(Debug, Clone)]
pub struct LvmConfig {
    /// LVM volume group name (e.g., "indexify-vg").
    pub volume_group: String,
    /// LVM thin pool LV name within the volume group (e.g., "thinpool").
    pub thin_pool: String,
}

/// Handle to the shared base image thin LV.
pub struct BaseImageHandle {
    /// LV name (e.g., "indexify-base").
    pub lv_name: String,
    /// Full device path (e.g., "/dev/{vg}/indexify-base").
    pub device_path: PathBuf,
    /// Size of the base image in bytes.
    pub size_bytes: u64,
}

/// Handle to a per-VM thin snapshot LV.
pub struct ThinSnapshotHandle {
    /// LV name (e.g., "indexify-vm-abc123").
    pub lv_name: String,
    /// Full device path (e.g., "/dev/{vg}/indexify-vm-abc123").
    pub device_path: PathBuf,
    /// Size of the thin LV in bytes.
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// LVM validation
// ---------------------------------------------------------------------------

/// Validate that the LVM volume group and thin pool exist.
pub fn validate_lvm_config(config: &LvmConfig) -> Result<()> {
    run_cmd("vgs", &[&config.volume_group])
        .with_context(|| format!("LVM volume group '{}' not found", config.volume_group))?;

    let pool_path = format!("{}/{}", config.volume_group, config.thin_pool);
    run_cmd("lvs", &[&pool_path])
        .with_context(|| format!("LVM thin pool '{}' not found", pool_path))?;

    tracing::info!(
        volume_group = %config.volume_group,
        thin_pool = %config.thin_pool,
        "LVM config validated"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Base image management
// ---------------------------------------------------------------------------

/// Set up the base image thin LV (idempotent — reuses existing if size
/// matches).
///
/// Imports the base rootfs file into a thin LV named "indexify-base".
/// If the LV already exists and matches the base image size, it is reused.
/// If the LV exists but has a different size, it is recreated.
pub fn setup_base_image(base_rootfs: &Path, lvm_config: &LvmConfig) -> Result<BaseImageHandle> {
    let lv_name = "indexify-base".to_string();
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));

    let base_size = std::fs::metadata(base_rootfs)
        .with_context(|| format!("Failed to stat base rootfs {}", base_rootfs.display()))?
        .len();

    // Check if the LV already exists.
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    if let Ok(output) = run_cmd(
        "lvs",
        &[
            "--noheadings",
            "--nosuffix",
            "--units",
            "b",
            "-o",
            "lv_size",
            &lv_path,
        ],
    ) {
        let existing_size: u64 = output.trim().parse().unwrap_or(0);
        // LVM rounds LV sizes up to extent boundaries (typically 4MB).
        // Accept any existing LV that is at least as large as the base image.
        if existing_size >= base_size {
            tracing::info!(
                lv_name = %lv_name,
                size_bytes = existing_size,
                "Reusing existing base image thin LV"
            );
            return Ok(BaseImageHandle {
                lv_name,
                device_path,
                size_bytes: existing_size,
            });
        }
        // Existing LV is too small — remove and recreate.
        tracing::info!(
            lv_name = %lv_name,
            existing_size,
            new_size = base_size,
            "Base image size changed, recreating thin LV"
        );
        let _ = run_cmd("lvremove", &["-f", &lv_path]);
    }

    // Create the thin LV.
    let pool_path = format!("{}/{}", lvm_config.volume_group, lvm_config.thin_pool);
    let size_arg = format!("{}B", base_size);
    run_cmd(
        "lvcreate",
        &["-V", &size_arg, "-T", &pool_path, "-n", &lv_name],
    )
    .with_context(|| format!("Failed to create base image thin LV {}", lv_path))?;

    // Import the base rootfs into the thin LV.
    run_cmd(
        "dd",
        &[
            &format!("if={}", base_rootfs.display()),
            &format!("of={}", device_path.display()),
            "bs=4M",
        ],
    )
    .with_context(|| format!("Failed to dd base rootfs into {}", device_path.display()))?;

    tracing::info!(
        lv_name = %lv_name,
        size_bytes = base_size,
        base_rootfs = %base_rootfs.display(),
        "Base image thin LV created and imported"
    );

    Ok(BaseImageHandle {
        lv_name,
        device_path,
        size_bytes: base_size,
    })
}

/// Tear down the base image thin LV.
pub fn teardown_base_image(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    run_cmd("lvremove", &["-f", &lv_path])
        .with_context(|| format!("Failed to remove base image thin LV {}", lv_path))?;
    tracing::info!(lv_name = %lv_name, "Base image thin LV removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-VM snapshot management
// ---------------------------------------------------------------------------

/// Create a thin snapshot for a VM from the base image.
///
/// Creates a native LVM thin snapshot, then optionally resizes if the
/// requested size exceeds the base image size.
pub fn create_snapshot(
    base: &BaseImageHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    let t0 = Instant::now();
    let lv_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));
    let base_lv_path = format!("{}/{}", lvm_config.volume_group, base.lv_name);

    // Create thin snapshot of the base image.
    run_cmd(
        "lvcreate",
        &["--snapshot", &base_lv_path, "--name", &lv_name],
    )
    .with_context(|| format!("Failed to create thin snapshot {}", lv_name))?;
    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "lvcreate --snapshot complete"
    );

    // Resize if requested size exceeds the base image.
    if size_bytes > base.size_bytes {
        let t_resize = Instant::now();
        let size_arg = format!("{}B", size_bytes);
        let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
        if let Err(e) = run_cmd("lvresize", &["-L", &size_arg, &lv_path]) {
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!("Failed to resize thin snapshot {}", lv_name)));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            size_bytes,
            elapsed_ms = t_resize.elapsed().as_millis() as u64,
            "lvresize complete"
        );

        // Check and resize the filesystem.
        let t_fsck = Instant::now();
        let dev_str = device_path.to_string_lossy().to_string();
        if let Err(e) = run_cmd("e2fsck", &["-f", "-y", &dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "e2fsck reported issues (continuing with resize)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_fsck.elapsed().as_millis() as u64,
            "e2fsck complete"
        );

        let t_resize2fs = Instant::now();
        if let Err(e) = run_cmd("resize2fs", &[&dev_str]) {
            let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!("Failed to resize2fs on {}", lv_name)));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_resize2fs.elapsed().as_millis() as u64,
            "resize2fs complete"
        );
    }

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        size_bytes,
        base_lv = %base.lv_name,
        total_ms = t0.elapsed().as_millis() as u64,
        "Thin snapshot created for VM"
    );

    Ok(ThinSnapshotHandle {
        lv_name,
        device_path,
        size_bytes,
    })
}

/// Create a thin snapshot from a delta file (snapshot restore path).
///
/// The delta file contains a header followed by block records (offset + length
/// + data) for blocks that differ from the base image. Only those blocks are
/// written into the thin snapshot; unchanged blocks stay as COW references to
/// the base LV. The delta file is deleted after import.
pub fn create_snapshot_from_delta(
    base: &BaseImageHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    delta_file: &Path,
    requested_size: u64,
) -> Result<ThinSnapshotHandle> {
    use std::os::unix::fs::FileExt;

    let t0 = Instant::now();
    let lv_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));
    let base_lv_path = format!("{}/{}", lvm_config.volume_group, base.lv_name);

    // Open delta file and read header.
    let mut delta = std::io::BufReader::new(
        std::fs::File::open(delta_file)
            .with_context(|| format!("Failed to open delta file {}", delta_file.display()))?,
    );
    let (_block_size, image_size) = read_delta_header(&mut delta)
        .with_context(|| format!("Failed to read delta header from {}", delta_file.display()))?;

    let lv_size = requested_size.max(image_size);

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        image_size,
        lv_size,
        delta_file = %delta_file.display(),
        "Starting snapshot restore from delta"
    );

    // Create thin snapshot of the base image.
    if let Err(e) = run_cmd(
        "lvcreate",
        &["--snapshot", &base_lv_path, "--name", &lv_name],
    ) {
        return Err(e.context(format!(
            "Failed to create thin snapshot for restored VM {}",
            lv_name
        )));
    }
    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "lvcreate --snapshot complete (restore)"
    );

    // Resize if the image or requested size exceeds the base.
    if lv_size > base.size_bytes {
        let t_resize = Instant::now();
        let size_arg = format!("{}B", lv_size);
        let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
        if let Err(e) = run_cmd("lvresize", &["-L", &size_arg, &lv_path]) {
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!(
                "Failed to resize snapshot for restored VM {}",
                lv_name
            )));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            lv_size,
            elapsed_ms = t_resize.elapsed().as_millis() as u64,
            "lvresize complete (restore)"
        );
    }

    // Apply delta block records to the thin snapshot.
    let t_apply = Instant::now();
    let target = std::fs::OpenOptions::new()
        .write(true)
        .open(&device_path)
        .with_context(|| format!("Failed to open target device {}", device_path.display()))?;

    let mut blocks_written: u64 = 0;
    let mut record_header = [0u8; 12]; // offset: u64 + length: u32
    let mut data = vec![0u8; BLOCK_SIZE]; // reused across iterations
    loop {
        match delta.read_exact(&mut record_header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
                let _ = run_cmd("lvremove", &["-f", &lv_path]);
                return Err(
                    anyhow::Error::from(e).context("Failed to read delta block record header")
                );
            }
        }

        let offset = u64::from_le_bytes(record_header[..8].try_into().unwrap());
        let length = u32::from_le_bytes(record_header[8..12].try_into().unwrap()) as usize;

        if length > BLOCK_SIZE {
            let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            bail!(
                "Corrupt delta: block record at offset {} has length {} (max {})",
                offset,
                length,
                BLOCK_SIZE
            );
        }

        if let Err(e) = delta.read_exact(&mut data[..length]) {
            let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(anyhow::Error::from(e).context(format!(
                "Failed to read delta block data at offset {}",
                offset
            )));
        }

        if let Err(e) = target.write_all_at(&data[..length], offset) {
            let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(anyhow::Error::from(e)
                .context(format!("Failed to write delta block at offset {}", offset)));
        }
        blocks_written += 1;
    }

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        blocks_written,
        elapsed_ms = t_apply.elapsed().as_millis() as u64,
        "Delta block records applied (COW preserved for unchanged blocks)"
    );

    // Delete the delta file now that data is in the LV.
    let _ = std::fs::remove_file(delta_file);

    // Resize filesystem if the LV is larger than the original image.
    if requested_size > image_size {
        let t_fsck = Instant::now();
        let dev_str = device_path.to_string_lossy().to_string();
        if let Err(e) = run_cmd("e2fsck", &["-f", "-y", &dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "e2fsck reported issues on restored image (continuing)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_fsck.elapsed().as_millis() as u64,
            "e2fsck complete (restore)"
        );

        let t_resize2fs = Instant::now();
        if let Err(e) = run_cmd("resize2fs", &[&dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "Failed to resize2fs on restored image (continuing)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_resize2fs.elapsed().as_millis() as u64,
            "resize2fs complete (restore)"
        );
    }

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        image_size,
        lv_size,
        blocks_written,
        total_ms = t0.elapsed().as_millis() as u64,
        "Thin snapshot created from delta"
    );

    Ok(ThinSnapshotHandle {
        lv_name,
        device_path,
        size_bytes: lv_size,
    })
}

/// Destroy a VM's thin snapshot LV.
pub fn destroy_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    run_cmd("lvremove", &["-f", &lv_path])
        .with_context(|| format!("Failed to remove thin LV {}", lv_path))?;
    tracing::info!(lv_name = %lv_name, "Thin snapshot LV removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// thin_delta metadata-based change detection
// ---------------------------------------------------------------------------

/// A byte range of changed blocks reported by `thin_delta`.
#[derive(Debug, Clone)]
pub struct ThinDeltaRange {
    /// Byte offset on the device.
    pub byte_offset: u64,
    /// Byte length of the changed region.
    pub byte_length: u64,
}

/// Get the thin device ID for an LV within the thin pool.
fn get_thin_id(config: &LvmConfig, lv_name: &str) -> Result<u32> {
    let lv_path = format!("{}/{}", config.volume_group, lv_name);
    let output = run_cmd("lvs", &["--noheadings", "-o", "thin_id", &lv_path])
        .with_context(|| format!("Failed to get thin_id for {}", lv_path))?;
    output.trim().parse::<u32>().with_context(|| {
        format!(
            "Failed to parse thin_id '{}' for {}",
            output.trim(),
            lv_path
        )
    })
}

/// Escape a VG and thin pool name into the device-mapper base name.
///
/// LVM doubles hyphens in VG/LV names and joins with a single hyphen:
/// VG "indexify-vg", pool "thinpool" → "indexify--vg-thinpool"
fn dm_base_name_for_pool(config: &LvmConfig) -> String {
    let vg_escaped = config.volume_group.replace('-', "--");
    let tpool_escaped = config.thin_pool.replace('-', "--");
    format!("{}-{}", vg_escaped, tpool_escaped)
}

/// Compute the device-mapper target name for the thin pool.
///
/// This is the target that accepts `dmsetup message` commands:
/// `{vg_escaped}-{tpool_escaped}-tpool`
fn dm_name_for_pool(config: &LvmConfig) -> String {
    format!("{}-tpool", dm_base_name_for_pool(config))
}

/// Reserve a point-in-time snapshot of the thin pool metadata btree.
fn reserve_metadata_snap(config: &LvmConfig) -> Result<()> {
    let pool_dm = dm_name_for_pool(config);
    run_cmd(
        "dmsetup",
        &["message", &pool_dm, "0", "reserve_metadata_snap"],
    )
    .with_context(|| format!("Failed to reserve metadata snap on {}", pool_dm))?;
    Ok(())
}

/// Release a previously reserved thin pool metadata snapshot.
fn release_metadata_snap(config: &LvmConfig) -> Result<()> {
    let pool_dm = dm_name_for_pool(config);
    run_cmd(
        "dmsetup",
        &["message", &pool_dm, "0", "release_metadata_snap"],
    )
    .with_context(|| format!("Failed to release metadata snap on {}", pool_dm))?;
    Ok(())
}

/// RAII guard that releases the thin pool metadata snapshot on drop.
struct MetadataSnapGuard {
    config: Option<LvmConfig>,
}

impl MetadataSnapGuard {
    fn new(config: &LvmConfig) -> Result<Self> {
        // Release any stale metadata snap left over from a prior crash.
        // This is a no-op if no snap is held (the error is ignored).
        let _ = release_metadata_snap(config);
        reserve_metadata_snap(config)?;
        Ok(Self {
            config: Some(config.clone()),
        })
    }

    /// Explicitly release the metadata snapshot and disarm the guard.
    fn release(mut self) -> Result<()> {
        if let Some(config) = self.config.take() {
            release_metadata_snap(&config)?;
        }
        Ok(())
    }
}

impl Drop for MetadataSnapGuard {
    fn drop(&mut self) {
        if let Some(ref config) = self.config {
            if let Err(e) = release_metadata_snap(config) {
                tracing::warn!(error = ?e, "Failed to release metadata snap in drop");
            }
        }
    }
}

/// Query `thin_delta` for the byte ranges that differ between two thin LVs.
///
/// Returns only blocks that are `<different>` (modified) or `<right_only>`
/// (newly allocated by the VM). Blocks that are `<same>` (unchanged) or
/// `<left_only>` (discarded by guest) are skipped.
pub fn query_thin_delta(
    config: &LvmConfig,
    base_lv: &str,
    vm_lv: &str,
) -> Result<Vec<ThinDeltaRange>> {
    let base_id = get_thin_id(config, base_lv)?;
    let vm_id = get_thin_id(config, vm_lv)?;

    let guard = MetadataSnapGuard::new(config)?;

    // Metadata device path: /dev/mapper/{vg_escaped}-{tpool_escaped}_tmeta
    let meta_dev = format!("/dev/mapper/{}_tmeta", dm_base_name_for_pool(config));

    let base_id_str = base_id.to_string();
    let vm_id_str = vm_id.to_string();
    let output = run_cmd(
        "thin_delta",
        &[
            "-m",
            "--snap1",
            &base_id_str,
            "--snap2",
            &vm_id_str,
            &meta_dev,
        ],
    )
    .with_context(|| {
        format!(
            "thin_delta failed (base_id={}, vm_id={}, meta_dev={})",
            base_id, vm_id, meta_dev
        )
    })?;

    // Explicitly release before parsing (guard is backup).
    guard.release()?;

    parse_thin_delta_xml(&output)
}

/// Parse thin_delta XML output into byte ranges.
///
/// Extracts `data_block_size` from the `<superblock>` element, then collects
/// `<different>` and `<right_only>` entries. Block offsets and lengths in the
/// XML are in units of `data_block_size * 512` bytes.
fn parse_thin_delta_xml(xml: &str) -> Result<Vec<ThinDeltaRange>> {
    // Extract data_block_size from <superblock ... data_block_size="128" ...>
    let data_block_size: u64 = xml
        .find("data_block_size=\"")
        .and_then(|pos| {
            let start = pos + "data_block_size=\"".len();
            let end = xml[start..].find('"')? + start;
            xml[start..end].parse::<u64>().ok()
        })
        .context("Failed to parse data_block_size from thin_delta output")?;

    let block_bytes = data_block_size * 512;
    let mut ranges = Vec::new();

    // Match <different .../> and <right_only .../> elements.
    for line in xml.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with("<different ") && !trimmed.starts_with("<right_only ") {
            continue;
        }

        let begin = extract_xml_attr(trimmed, "begin")
            .with_context(|| format!("Missing 'begin' in: {}", trimmed))?;
        let length = extract_xml_attr(trimmed, "length")
            .with_context(|| format!("Missing 'length' in: {}", trimmed))?;

        ranges.push(ThinDeltaRange {
            byte_offset: begin * block_bytes,
            byte_length: length * block_bytes,
        });
    }

    // Sort by offset for sequential I/O.
    ranges.sort_by_key(|r| r.byte_offset);

    tracing::info!(
        data_block_size,
        block_bytes,
        num_ranges = ranges.len(),
        total_changed_bytes = ranges.iter().map(|r| r.byte_length).sum::<u64>(),
        "Parsed thin_delta metadata"
    );

    Ok(ranges)
}

/// Extract a numeric attribute value from a simple XML element string.
/// E.g., `extract_xml_attr(r#"<different begin="10" length="5"/>"#, "begin")` →
/// `Some(10)`.
fn extract_xml_attr(element: &str, attr: &str) -> Option<u64> {
    let pattern = format!("{}=\"", attr);
    let pos = element.find(&pattern)?;
    let start = pos + pattern.len();
    let end = element[start..].find('"')? + start;
    element[start..end].parse::<u64>().ok()
}

// ---------------------------------------------------------------------------
// Suspend/resume for snapshotter
// ---------------------------------------------------------------------------

/// Convert an LV name to its device-mapper name.
///
/// LVM's device-mapper names double all hyphens in the VG and LV names,
/// then join them with a single hyphen. For example:
/// VG "indexify-vg", LV "indexify-vm-abc" → "indexify--vg-indexify--vm--abc"
fn dm_name_for_lv(config: &LvmConfig, lv_name: &str) -> String {
    let vg_escaped = config.volume_group.replace('-', "--");
    let lv_escaped = lv_name.replace('-', "--");
    format!("{}-{}", vg_escaped, lv_escaped)
}

/// Suspend a thin LV device, flushing all pending I/O.
///
/// After suspension the device is consistent and can be read directly.
/// The device must be resumed or removed afterwards.
pub fn suspend_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let dm_name = dm_name_for_lv(lvm_config, lv_name);

    run_cmd("dmsetup", &["suspend", &dm_name])
        .with_context(|| format!("Failed to suspend dm device {}", dm_name))?;
    tracing::info!(lv_name = %lv_name, dm_name = %dm_name, "Thin LV suspended");
    Ok(())
}

/// Async version of suspend_snapshot.
pub async fn suspend_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || suspend_snapshot(&lv_name, &lvm_config))
        .await
        .context("suspend_snapshot task panicked")?
}

/// Resume a previously suspended thin LV device.
pub fn resume_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let dm_name = dm_name_for_lv(lvm_config, lv_name);

    run_cmd("dmsetup", &["resume", &dm_name])
        .with_context(|| format!("Failed to resume dm device {}", dm_name))?;
    tracing::info!(lv_name = %lv_name, dm_name = %dm_name, "Thin LV resumed");
    Ok(())
}

/// Async version of resume_snapshot.
pub async fn resume_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || resume_snapshot(&lv_name, &lvm_config))
        .await
        .context("resume_snapshot task panicked")?
}

// ---------------------------------------------------------------------------
// Stale device cleanup
// ---------------------------------------------------------------------------

/// Clean up stale `indexify-vm-*` thin LVs from previous runs.
///
/// Only removes VM LVs whose VM ID is NOT in `active_vm_ids`.
pub fn cleanup_stale_devices(active_vm_ids: &HashSet<String>, lvm_config: &LvmConfig) {
    let vg = &lvm_config.volume_group;
    if let Ok(output) = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]) {
        for line in output.lines() {
            let lv_name = line.trim();

            if let Some(vm_id) = lv_name.strip_prefix("indexify-vm-") {
                if !active_vm_ids.contains(vm_id) {
                    let lv_path = format!("{}/{}", vg, lv_name);
                    if let Err(e) = run_cmd("lvremove", &["-f", &lv_path]) {
                        tracing::warn!(
                            lv_name = %lv_name,
                            error = ?e,
                            "Failed to remove stale VM thin LV"
                        );
                    } else {
                        tracing::info!(
                            lv_name = %lv_name,
                            "Removed stale VM thin LV"
                        );
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Async variants
// ---------------------------------------------------------------------------

/// Async version of create_snapshot (runs blocking IO on spawn_blocking).
pub async fn create_snapshot_async(
    base_lv_name: String,
    base_device_path: PathBuf,
    base_size_bytes: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let base = BaseImageHandle {
            lv_name: base_lv_name,
            device_path: base_device_path,
            size_bytes: base_size_bytes,
        };
        create_snapshot(&base, &vm_id, &lvm_config, size_bytes)
    })
    .await
    .context("create_snapshot task panicked")?
}

/// Async version of create_snapshot_from_delta.
pub async fn create_snapshot_from_delta_async(
    base_lv_name: String,
    base_device_path: PathBuf,
    base_size_bytes: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    delta_file: PathBuf,
    requested_size: u64,
) -> Result<ThinSnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let base = BaseImageHandle {
            lv_name: base_lv_name,
            device_path: base_device_path,
            size_bytes: base_size_bytes,
        };
        create_snapshot_from_delta(&base, &vm_id, &lvm_config, &delta_file, requested_size)
    })
    .await
    .context("create_snapshot_from_delta task panicked")?
}

/// Async version of destroy_snapshot.
pub async fn destroy_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || destroy_snapshot(&lv_name, &lvm_config))
        .await
        .context("destroy_snapshot task panicked")?
}

// ---------------------------------------------------------------------------
// Command helpers
// ---------------------------------------------------------------------------

/// Run a command and capture stdout. Returns error if exit code != 0.
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String> {
    let output = std::process::Command::new(cmd)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .with_context(|| format!("Failed to execute: {} {:?}", cmd, args))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} {:?} failed (exit {}): {}",
            cmd,
            args,
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
