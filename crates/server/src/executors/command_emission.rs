use std::collections::HashSet;

use tracing::{error, trace};

use super::ExecutorManager;
use crate::{
    data_model::{self, ExecutorId, SandboxKey},
    executor_api::executor_api_pb::{self, Allocation, FunctionRef},
    pb_helpers::{blob_store_path_to_url, string_to_data_payload_encoding},
    state_store::{
        SchedulerCommandIntent,
        in_memory_state::InMemoryState,
        requests::SchedulerUpdateRequest,
    },
};

impl ExecutorManager {
    fn container_command_priority(
        container_id: &data_model::ContainerId,
        container_meta: &data_model::ContainerServerMetadata,
        updated_sandbox_container_ids: &HashSet<data_model::ContainerId>,
    ) -> u8 {
        if matches!(
            container_meta.desired_state,
            data_model::ContainerState::Terminated { .. }
        ) {
            // Emit removals first so resource teardown happens before creates.
            return 0;
        }

        if container_meta.container_type == data_model::ContainerType::Sandbox &&
            updated_sandbox_container_ids.contains(container_id)
        {
            // Metadata-only warm-claim updates should happen after removals but
            // before adds.
            return 1;
        }

        if matches!(
            container_meta.function_container.state,
            data_model::ContainerState::Pending
        ) {
            return 2;
        }

        // No command emitted.
        3
    }

    fn commands_from_full_snapshot(
        &self,
        snapshot: super::ExecutorStateSnapshot,
    ) -> Vec<executor_api_pb::Command> {
        let mut commands = Vec::new();

        for container in snapshot.containers {
            commands.push(executor_api_pb::Command {
                seq: 0,
                command: Some(executor_api_pb::command::Command::AddContainer(
                    executor_api_pb::AddContainer {
                        container: Some(container),
                    },
                )),
            });
        }

        for allocation in snapshot.allocations {
            commands.push(executor_api_pb::Command {
                seq: 0,
                command: Some(executor_api_pb::command::Command::RunAllocation(
                    executor_api_pb::RunAllocation {
                        allocation: Some(allocation),
                    },
                )),
            });
        }

        for pending_snapshot in snapshot.pending_snapshots {
            commands.push(executor_api_pb::Command {
                seq: 0,
                command: Some(executor_api_pb::command::Command::SnapshotContainer(
                    executor_api_pb::SnapshotContainer {
                        container_id: pending_snapshot.container_id,
                        snapshot_id: pending_snapshot.snapshot_id,
                        upload_uri: pending_snapshot.upload_uri,
                    },
                )),
            });
        }

        commands
    }

    fn allocation_to_proto(&self, allocation: &data_model::Allocation) -> Allocation {
        let mut args = Vec::new();
        let blob_store_url_schema = self
            .blob_store_registry
            .get_blob_store(&allocation.namespace)
            .get_url_scheme();
        let blob_store_url = self
            .blob_store_registry
            .get_blob_store(&allocation.namespace)
            .get_url();
        for input_arg in &allocation.input_args {
            args.push(executor_api_pb::DataPayload {
                id: Some(input_arg.data_payload.id.clone()),
                uri: Some(blob_store_path_to_url(
                    &input_arg.data_payload.path,
                    &blob_store_url_schema,
                    &blob_store_url,
                )),
                size: Some(input_arg.data_payload.size),
                sha256_hash: Some(input_arg.data_payload.sha256_hash.clone()),
                encoding: Some(
                    string_to_data_payload_encoding(&input_arg.data_payload.encoding).into(),
                ),
                encoding_version: Some(0),
                offset: Some(input_arg.data_payload.offset),
                metadata_size: Some(input_arg.data_payload.metadata_size),
                source_function_call_id: input_arg
                    .function_call_id
                    .as_ref()
                    .map(|id| id.to_string()),
                content_type: Some(input_arg.data_payload.encoding.clone()),
            });
        }
        let request_data_payload_uri_prefix = format!(
            "{}/{}",
            blob_store_url,
            data_model::DataPayload::request_key_prefix(
                &allocation.namespace,
                &allocation.application,
                &allocation.request_id,
            ),
        );

        Allocation {
            function: Some(FunctionRef {
                namespace: Some(allocation.namespace.clone()),
                application_name: Some(allocation.application.clone()),
                function_name: Some(allocation.function.clone()),
                application_version: None,
            }),
            container_id: Some(allocation.target.container_id.get().to_string()),
            allocation_id: Some(allocation.id.to_string()),
            function_call_id: Some(allocation.function_call_id.to_string()),
            request_id: Some(allocation.request_id.to_string()),
            args,
            request_data_payload_uri_prefix: Some(request_data_payload_uri_prefix.clone()),
            request_error_payload_uri_prefix: Some(request_data_payload_uri_prefix),
            function_call_metadata: Some(allocation.call_metadata.clone().into()),
            replay_mode: None,
            last_event_clock: None,
        }
    }

    /// Emit a full desired-state command batch for one executor.
    ///
    /// When `force_full_sync` is true, reset the persisted outbox first
    /// (reconnect/local-state-loss recovery).
    pub async fn emit_commands_for_executor(
        &self,
        executor_id: &ExecutorId,
        force_full_sync: bool,
    ) {
        let conn = {
            let connections = self.indexify_state.executor_connections.read().await;
            let Some(conn) = connections.get(executor_id).cloned() else {
                return;
            };
            conn
        };
        let _emit_guard = conn.command_emit_lock.lock().await;

        if force_full_sync {
            if let Err(err) = self
                .indexify_state
                .reset_executor_command_outbox(executor_id)
                .await
            {
                error!(
                    executor_id = executor_id.get(),
                    error = ?err,
                    "failed to reset persistent command outbox for full sync"
                );
            }
            conn.reset_for_full_sync().await;
        }

        let Some(snapshot) = self.get_executor_state(executor_id).await else {
            trace!(
                executor_id = executor_id.get(),
                "emit_commands_for_executor: desired state unavailable"
            );
            return;
        };

        let commands = self.commands_from_full_snapshot(snapshot);

        if !commands.is_empty() &&
            let Err(err) = self
                .indexify_state
                .enqueue_executor_commands(executor_id, commands)
                .await
        {
            error!(
                executor_id = executor_id.get(),
                error = ?err,
                "failed to enqueue commands into persistent outbox"
            );
        }
    }

    pub fn rebuild_scheduler_command_intents(
        &self,
        update: &mut SchedulerUpdateRequest,
        indexes: &InMemoryState,
    ) {
        update.scheduler_command_intents.clear();
        let mut intents = Vec::new();
        let updated_sandbox_container_ids: HashSet<_> = update
            .updated_sandboxes
            .values()
            .filter_map(|sandbox| sandbox.container_id.clone())
            .collect();
        let mut ordered_containers: Vec<_> = update.containers.iter().collect();
        ordered_containers.sort_by(
            |(container_id_a, container_meta_a), (container_id_b, container_meta_b)| {
                let priority_a = Self::container_command_priority(
                    container_id_a,
                    container_meta_a,
                    &updated_sandbox_container_ids,
                );
                let priority_b = Self::container_command_priority(
                    container_id_b,
                    container_meta_b,
                    &updated_sandbox_container_ids,
                );
                priority_a
                    .cmp(&priority_b)
                    .then_with(|| container_id_a.get().cmp(container_id_b.get()))
            },
        );

        for (container_id, container_meta) in ordered_containers {
            let command = if matches!(
                container_meta.desired_state,
                data_model::ContainerState::Terminated { .. }
            ) {
                executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::RemoveContainer(
                        executor_api_pb::RemoveContainer {
                            container_id: container_id.get().to_string(),
                            reason: None,
                        },
                    )),
                }
            } else {
                // Only emit AddContainer when the scheduler creates a new
                // container (Pending). Pure runtime metadata updates (e.g.
                // allocation bookkeeping, heartbeat state reflection) must not
                // re-emit AddContainer.
                if matches!(
                    container_meta.function_container.state,
                    data_model::ContainerState::Pending
                ) {
                    let Some(container_pb) =
                        self.build_container_description_from_meta(indexes, container_meta)
                    else {
                        continue;
                    };
                    executor_api_pb::Command {
                        seq: 0,
                        command: Some(executor_api_pb::command::Command::AddContainer(
                            executor_api_pb::AddContainer {
                                container: Some(container_pb),
                            },
                        )),
                    }
                } else if container_meta.container_type == data_model::ContainerType::Sandbox &&
                    updated_sandbox_container_ids.contains(container_id)
                {
                    // Warm pool claims keep containers Running and only mutate
                    // sandbox metadata (sandbox_id/timeout). Emit a targeted
                    // metadata update so the dataplane can apply the claim.
                    let Some(container_pb) =
                        self.build_container_description_from_meta(indexes, container_meta)
                    else {
                        continue;
                    };
                    executor_api_pb::Command {
                        seq: 0,
                        command: Some(
                            executor_api_pb::command::Command::UpdateContainerDescription(
                                executor_api_pb::UpdateContainerDescription {
                                    container_id: container_id.get().to_string(),
                                    sandbox_metadata: container_pb.sandbox_metadata,
                                },
                            ),
                        ),
                    }
                } else {
                    continue;
                }
            };
            intents.push(SchedulerCommandIntent {
                executor_id: container_meta.executor_id.clone(),
                command,
            });
        }

        for allocation in &update.new_allocations {
            intents.push(SchedulerCommandIntent {
                executor_id: allocation.target.executor_id.clone(),
                command: executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::RunAllocation(
                        executor_api_pb::RunAllocation {
                            allocation: Some(self.allocation_to_proto(allocation)),
                        },
                    )),
                },
            });
        }

        for snapshot in update.updated_snapshots.values() {
            let sandbox_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
            let sandbox = update
                .updated_sandboxes
                .get(&sandbox_key)
                .or_else(|| indexes.sandboxes.get(&sandbox_key).map(Box::as_ref));
            let Some(sandbox) = sandbox else {
                continue;
            };
            let Some(snapshot_executor_id) = &sandbox.executor_id else {
                continue;
            };
            if snapshot.status != data_model::SnapshotStatus::InProgress {
                continue;
            }
            let Some(upload_uri) = snapshot.upload_uri.clone() else {
                continue;
            };
            let Some(container_id) = sandbox.container_id.as_ref() else {
                continue;
            };
            intents.push(SchedulerCommandIntent {
                executor_id: snapshot_executor_id.clone(),
                command: executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::SnapshotContainer(
                        executor_api_pb::SnapshotContainer {
                            container_id: container_id.get().to_string(),
                            snapshot_id: snapshot.id.get().to_string(),
                            upload_uri,
                        },
                    )),
                },
            });
        }

        update.scheduler_command_intents.extend(intents);
    }

    pub async fn drain_and_emit_scheduler_command_intents(&self) {
        const PAGE_SIZE: usize = 256;
        let started_at = std::time::Instant::now();
        let mut total_drained = 0u64;
        self.scheduler_command_intent_backlog.record(
            self.indexify_state.scheduler_command_intent_backlog_len(),
            &[],
        );

        loop {
            let enqueued = match self
                .indexify_state
                .move_scheduler_command_intents_to_outbox(PAGE_SIZE)
                .await
            {
                Ok(enqueued) => enqueued,
                Err(err) => {
                    error!(
                        error = ?err,
                        "failed to drain persisted scheduler command intents"
                    );
                    break;
                }
            };
            if enqueued.is_empty() {
                break;
            }
            let batch_drained: u64 = enqueued.values().map(|cmds| cmds.len() as u64).sum();
            total_drained = total_drained.saturating_add(batch_drained);
            for (executor_id, commands) in enqueued {
                let conn = {
                    let connections = self.indexify_state.executor_connections.read().await;
                    connections.get(&executor_id).cloned()
                };
                let Some(conn) = conn else {
                    continue;
                };
                let _emit_guard = conn.command_emit_lock.lock().await;
                if let Err(err) = conn.push_commands(commands).await {
                    error!(
                        executor_id = executor_id.get(),
                        error = ?err,
                        "failed to enqueue commands for executor poll delivery"
                    );
                }
            }
        }
        if total_drained > 0 {
            self.scheduler_command_intent_drained_total
                .add(total_drained, &[]);
        }
        self.scheduler_command_intent_drain_latency
            .record(started_at.elapsed().as_secs_f64(), &[]);
        self.scheduler_command_intent_backlog.record(
            self.indexify_state.scheduler_command_intent_backlog_len(),
            &[],
        );
    }
}
