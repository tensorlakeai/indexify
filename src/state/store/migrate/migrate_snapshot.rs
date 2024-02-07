use serde::{Deserialize, Serialize};
use strum::EnumIter;

use crate::state::store::state_machine_objects::IndexifyState;

pub const CURRENT_SNAPSHOT_VERSION: SnapshotVersion = SnapshotVersion::V1;

#[derive(
    Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Ord, PartialOrd, EnumIter,
)]
pub enum SnapshotVersion {
    #[default]
    V0,
    V1,
    // ...V3, V4, V5, etc.
    // Add new snapshot versions here when a
    // backward-incompatible change is made to the Indexify State
}

/// Perform snapshot version migrations. 
/// This function is called when a snapshot is loaded from disk.
pub fn migrate_snapshot_to_version(
    version: SnapshotVersion,
    snapshot: &mut IndexifyState,
) -> anyhow::Result<()> {
    match version {
        SnapshotVersion::V0 => {}
        SnapshotVersion::V1 => {
            // In V1, Namespaces were introduced. Migrate the existing repositories to
            // namespaces and clear the repositories field.
            snapshot.namespaces = snapshot.repositories.clone();
            snapshot.repositories.clear();
        } // Add future migrations here
    }
    Ok(())
}
