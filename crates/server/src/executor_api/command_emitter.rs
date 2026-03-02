#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use crate::{executor_api::executor_api_pb, executors::ExecutorStateSnapshot};

/// Pure, stateful diff engine that compares the current `ExecutorStateSnapshot`
/// against what it previously saw and produces typed `Command` messages.
///
/// On the first call, the tracking sets are empty so everything is "new" ---
/// producing AddContainer + RunAllocation for full state (equivalent to
/// initial sync).
///
/// `command_seq = 0` means the command is informational / unsolicited.
#[derive(Clone)]
pub struct CommandEmitter {
    next_seq: u64,
    /// Container descriptions sent via AddContainer, keyed by container ID.
    /// Tracked as full descriptions so we can detect changes and emit
    /// `UpdateContainerDescription` commands.
    pub(crate) known_containers: HashMap<String, executor_api_pb::ContainerDescription>,
    /// Allocation IDs sent via RunAllocation.
    pub(crate) known_allocations: HashSet<String>,
    /// Snapshot IDs sent via SnapshotContainer.
    pub(crate) known_snapshot_ids: HashSet<String>,
    /// Whether this emitter has completed at least one full sync
    /// (commit_snapshot). `false` on initial creation or after
    /// re-registration. `true` after the first successful
    /// `commit_snapshot`. Used by the command generator task to decide:
    /// - `has_synced == false` -> initial full sync needed
    /// - `has_synced == true` -> skip full sync, drain buffered events
    pub has_synced: bool,
}

impl CommandEmitter {
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            known_containers: HashMap::new(),
            known_allocations: HashSet::new(),
            known_snapshot_ids: HashSet::new(),
            has_synced: false,
        }
    }

    pub(crate) fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Diff the current desired state against what was previously seen and
    /// produce a batch of `Command` messages for the delta.
    ///
    /// **Command ordering** matches what `emit_scheduler_events` guarantees:
    /// 1. REMOVALS first — free GPU/memory before new containers claim them
    /// 2. ADDITIONS — new containers
    /// 3. UPDATES — changed descriptions (sandbox_metadata)
    /// 4. ALLOCATIONS — must come after AddContainer so container exists
    /// 5. SNAPSHOTS — new snapshot commands
    ///
    /// **Important**: this does NOT update the emitter's tracking state.
    /// Call [`commit_snapshot`] after all commands have been successfully
    /// delivered to the client so that the tracking sets stay accurate if
    /// delivery fails partway through.
    pub fn emit_commands(
        &mut self,
        snapshot: &ExecutorStateSnapshot,
    ) -> Vec<executor_api_pb::Command> {
        let mut commands = Vec::new();

        let current_containers: HashMap<String, executor_api_pb::ContainerDescription> = snapshot
            .containers
            .iter()
            .filter_map(|fe| fe.id.clone().map(|id| (id, fe.clone())))
            .collect();

        // 1. REMOVALS first — free GPU/memory before new containers claim them
        let removed_containers: Vec<String> = self
            .known_containers
            .keys()
            .filter(|id| !current_containers.contains_key(*id))
            .cloned()
            .collect();
        for id in removed_containers {
            let seq = self.next_seq();
            commands.push(executor_api_pb::Command {
                seq,
                command: Some(executor_api_pb::command::Command::RemoveContainer(
                    executor_api_pb::RemoveContainer {
                        container_id: id,
                        reason: None,
                    },
                )),
            });
        }

        // 2. ADDITIONS — new containers
        for fe in &snapshot.containers {
            if let Some(id) = &fe.id &&
                !self.known_containers.contains_key(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::AddContainer(
                        executor_api_pb::AddContainer {
                            container: Some(fe.clone()),
                        },
                    )),
                });
            }
        }

        // 3. UPDATES — changed descriptions (sandbox_metadata)
        for fe in &snapshot.containers {
            if let Some(id) = &fe.id &&
                let Some(known) = self.known_containers.get(id) &&
                known != fe
            {
                let mut update = executor_api_pb::UpdateContainerDescription {
                    container_id: id.clone(),
                    sandbox_metadata: None,
                };
                if known.sandbox_metadata != fe.sandbox_metadata {
                    update.sandbox_metadata = fe.sandbox_metadata.clone();
                }
                if update.sandbox_metadata.is_some() {
                    let seq = self.next_seq();
                    commands.push(executor_api_pb::Command {
                        seq,
                        command: Some(
                            executor_api_pb::command::Command::UpdateContainerDescription(update),
                        ),
                    });
                }
            }
        }

        // 4. ALLOCATIONS — must come after AddContainer so container exists
        for allocation in &snapshot.allocations {
            if let Some(id) = &allocation.allocation_id &&
                !self.known_allocations.contains(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::RunAllocation(
                        executor_api_pb::RunAllocation {
                            allocation: Some(allocation.clone()),
                        },
                    )),
                });
            }
        }

        // 5. SNAPSHOTS — new snapshot commands
        for snap in &snapshot.pending_snapshots {
            if !self.known_snapshot_ids.contains(&snap.snapshot_id) {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::SnapshotContainer(
                        executor_api_pb::SnapshotContainer {
                            container_id: snap.container_id.clone(),
                            snapshot_id: snap.snapshot_id.clone(),
                            upload_uri: snap.upload_uri.clone(),
                        },
                    )),
                });
            }
        }

        commands
    }

    /// Commit the snapshot to the emitter's tracking state.
    ///
    /// Call this only after all commands from [`emit_commands`] have been
    /// successfully delivered to the client.  If delivery fails partway
    /// through, skipping this call ensures the next full sync re-emits the
    /// missing commands.
    pub fn commit_snapshot(&mut self, snapshot: &ExecutorStateSnapshot) {
        self.known_containers = snapshot
            .containers
            .iter()
            .filter_map(|fe| fe.id.clone().map(|id| (id, fe.clone())))
            .collect();

        // Allocations that disappear are completed, not killed. We just stop
        // tracking.
        self.known_allocations = snapshot
            .allocations
            .iter()
            .filter_map(|a| a.allocation_id.clone())
            .collect();

        self.known_snapshot_ids = snapshot
            .pending_snapshots
            .iter()
            .map(|s| s.snapshot_id.clone())
            .collect();

        self.has_synced = true;
    }
}
