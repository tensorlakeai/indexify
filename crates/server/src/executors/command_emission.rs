use tracing::{error, trace};

use super::ExecutorManager;
use crate::{
    data_model::{self, ExecutorId, SandboxKey},
    executor_api::executor_api_pb::{self, Allocation, FunctionRef},
    pb_helpers::{blob_store_path_to_url, string_to_data_payload_encoding},
    state_store::requests::SchedulerUpdateRequest,
};

impl ExecutorManager {
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

    /// Emit commands only for executors affected by the scheduler update.
    pub async fn emit_commands_from_scheduler_update(&self, update: &SchedulerUpdateRequest) {
        let affected = self
            .indexify_state
            .affected_executors_from_update(update)
            .await;
        let app_state = self.indexify_state.app_state.load();
        for executor_id in affected {
            let conn = {
                let connections = self.indexify_state.executor_connections.read().await;
                let Some(conn) = connections.get(&executor_id).cloned() else {
                    continue;
                };
                conn
            };
            let _emit_guard = conn.command_emit_lock.lock().await;
            let mut commands = Vec::new();

            for (container_id, container_meta) in &update.containers {
                if container_meta.executor_id != executor_id {
                    continue;
                }

                if matches!(
                    container_meta.desired_state,
                    data_model::ContainerState::Terminated { .. }
                ) {
                    commands.push(executor_api_pb::Command {
                        seq: 0,
                        command: Some(executor_api_pb::command::Command::RemoveContainer(
                            executor_api_pb::RemoveContainer {
                                container_id: container_id.get().to_string(),
                                reason: None,
                            },
                        )),
                    });
                    continue;
                }

                let Some(container_pb) =
                    self.build_container_description_from_meta(&app_state, container_meta)
                else {
                    continue;
                };
                commands.push(executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::AddContainer(
                        executor_api_pb::AddContainer {
                            container: Some(container_pb),
                        },
                    )),
                });
            }

            for allocation in &update.new_allocations {
                if allocation.target.executor_id != executor_id {
                    continue;
                }
                commands.push(executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::RunAllocation(
                        executor_api_pb::RunAllocation {
                            allocation: Some(self.allocation_to_proto(allocation)),
                        },
                    )),
                });
            }

            for snapshot in update.updated_snapshots.values() {
                let sandbox_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
                let Some(sandbox) = app_state.indexes.sandboxes.get(&sandbox_key) else {
                    continue;
                };
                let Some(snapshot_executor_id) = &sandbox.executor_id else {
                    continue;
                };
                if *snapshot_executor_id != executor_id {
                    continue;
                }

                if snapshot.status != data_model::SnapshotStatus::InProgress {
                    continue;
                }
                let Some(upload_uri) = snapshot.upload_uri.clone() else {
                    continue;
                };
                let Some(container_id) = sandbox.container_id.as_ref() else {
                    continue;
                };
                commands.push(executor_api_pb::Command {
                    seq: 0,
                    command: Some(executor_api_pb::command::Command::SnapshotContainer(
                        executor_api_pb::SnapshotContainer {
                            container_id: container_id.get().to_string(),
                            snapshot_id: snapshot.id.get().to_string(),
                            upload_uri,
                        },
                    )),
                });
            }

            if !commands.is_empty() &&
                let Err(err) = self
                    .indexify_state
                    .enqueue_executor_commands(&executor_id, commands)
                    .await
            {
                error!(
                    executor_id = executor_id.get(),
                    error = ?err,
                    "failed to enqueue scheduler-derived command batch"
                );
                continue;
            }
        }
    }
}
