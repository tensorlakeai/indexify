pub mod migrate_snapshot;

use strum::IntoEnumIterator;

pub use self::migrate_snapshot::{
    migrate_snapshot_to_version,
    SnapshotVersion,
    CURRENT_SNAPSHOT_VERSION,
};

use super::state_machine_objects::IndexifyState;

pub fn migrate_snapshot_to_most_recent_version(
    snapshot_version: SnapshotVersion,
    snapshot_state: &mut IndexifyState,
) -> Result<SnapshotVersion, anyhow::Error> {
    let mut current_version = snapshot_version;
    let target_version = CURRENT_SNAPSHOT_VERSION;

    for version in SnapshotVersion::iter() {
        if version > current_version && version <= target_version {
            migrate_snapshot_to_version(version, snapshot_state)?;
            current_version = version; // Update the current version after
                                       // successful migration
        }
    }

    // snapshot_state.version = current_version;

    Ok(current_version)
}
