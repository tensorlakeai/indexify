use std::{sync::Arc, time::Duration, vec};

use anyhow::Result;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use tokio::sync::Notify;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    data_model::{self, Application, ApplicationState, ChangeType, SnapshotStatus, StateChange},
    metrics::{Timer, low_latency_boundaries},
    processor::{
        buffer_reconciler::BufferReconciler,
        container_reconciler,
        function_run_creator,
        function_run_processor::FunctionRunProcessor,
        sandbox_processor::SandboxProcessor,
    },
    state_store::{
        IndexifyState,
        requests::{
            CreateOrUpdateApplicationRequest,
            DeleteApplicationRequest,
            DeleteContainerPoolRequest,
            DeleteRequestRequest,
            FailSnapshotRequest,
            RequestPayload,
            SchedulerUpdatePayload,
            SchedulerUpdateRequest,
            StateMachineUpdateRequest,
        },
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ns},
};

pub struct ApplicationProcessor {
    pub indexify_state: Arc<IndexifyState>,
    pub write_sm_update_latency: Histogram<f64>,
    pub state_change_latency: Histogram<f64>,
    pub state_changes_total: Counter<u64>,
    pub state_transition_latency: Histogram<f64>,
    pub handle_state_change_latency: Histogram<f64>,
    pub allocate_function_runs_latency: Histogram<f64>,
    pub state_change_queue_depth: Gauge<u64>,
    pub queue_size: u32,
    pub cluster_vacuum_interval: Duration,
    pub cluster_vacuum_max_idle_age: Duration,
    pub snapshot_timeout: Duration,
}

impl ApplicationProcessor {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        queue_size: u32,
        cluster_vacuum_interval: Duration,
        cluster_vacuum_max_idle_age: Duration,
        snapshot_timeout: Duration,
    ) -> Self {
        let meter = opentelemetry::global::meter("processor_metrics");

        let write_sm_update_latency = meter
            .f64_histogram("indexify.application_processor.write_sm_update_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of writing state machine update in seconds")
            .build();

        let state_change_latency = meter
            .f64_histogram("indexify.application_processor.state_change_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state change processing in seconds")
            .build();

        let state_changes_total = meter
            .u64_counter("indexify.application_processor.state_changes_total")
            .with_description("Total number of state changes processed")
            .build();

        let state_transition_latency = meter
            .f64_histogram("indexify.application_processor.state_transition_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state transition since state change creation in seconds")
            .build();

        let handle_state_change_latency = meter
            .f64_histogram("indexify.application_processor.handle_state_change_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of state change handling in seconds")
            .build();

        let allocate_function_runs_latency = meter
            .f64_histogram("indexify.application_processor.allocate_function_runs_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Latency of function runs allocation in seconds")
            .build();

        let state_change_queue_depth = meter
            .u64_gauge("indexify.state_store.state_change_queue_depth")
            .with_description("Number of unprocessed state changes in the queue")
            .build();

        Self {
            indexify_state,
            write_sm_update_latency,
            state_change_latency,
            state_changes_total,
            state_transition_latency,
            handle_state_change_latency,
            allocate_function_runs_latency,
            state_change_queue_depth,
            queue_size,
            cluster_vacuum_interval,
            cluster_vacuum_max_idle_age,
            snapshot_timeout,
        }
    }

    pub async fn validate_app_constraints(&self) -> Result<()> {
        let updated_applications = {
            let in_memory_state = self.indexify_state.in_memory_state.read().await;
            let executor_catalog = &in_memory_state.executor_catalog;
            in_memory_state.applications.values().filter_map(|application| {
                let target_state = if application.can_be_scheduled(executor_catalog).is_ok() {
                        ApplicationState::Active
                } else {
                        ApplicationState::Disabled{reason: "The application contains functions that have unsatisfiable placement constraints".to_string()}
                };

                if target_state != application.state {
                    let mut updated_application = *application.clone();
                    updated_application.state = target_state;
                    Some(updated_application)
                } else {
                    None
                }
            })
                .collect::<Vec<Application>>()
        };

        for application in updated_applications {
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                        CreateOrUpdateApplicationRequest {
                            namespace: application.namespace.clone(),
                            application,
                            upgrade_requests_to_current_version: true,
                            container_pools: vec![],
                        },
                    )),
                })
                .await?;
        }
        Ok(())
    }

    #[instrument(skip(self, shutdown_rx))]
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut cached_state_changes: Vec<StateChange> = vec![];
        let mut change_events_rx = self.indexify_state.change_events_rx.clone();
        let mut last_global_state_change_cursor: Option<Vec<u8>> = None;
        let mut last_namespace_state_change_cursor: Option<Vec<u8>> = None;
        // Used to run the loop when there are more than 1 change events queued up
        // The watch from the state store only notifies that there are N number of state
        // changes but if we only process one event from the queue then the
        // watch will not notify again
        let notify = Arc::new(Notify::new());

        let vacuum_enabled = !self.cluster_vacuum_interval.is_zero();
        let mut vacuum_timer = tokio::time::interval(
            if vacuum_enabled {
                self.cluster_vacuum_interval
            } else {
                // Interval must be > 0; use a large dummy value (guarded by vacuum_enabled)
                Duration::from_secs(3600)
            },
        );
        // Consume the first immediate tick so vacuum doesn't fire on startup
        vacuum_timer.tick().await;

        loop {
            tokio::select! {
                _ = change_events_rx.changed() => {
                    change_events_rx.borrow_and_update();
                    if let Err(err) = self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        error!("error processing state change: {:?}", err);
                        continue
                    }
                },
                _ = notify.notified() => {
                    if let Err(err) = self.write_sm_update(&mut cached_state_changes, &mut last_global_state_change_cursor, &mut last_namespace_state_change_cursor, &notify).await {
                        error!("error processing state change: {:?}", err);
                        continue
                    }
                },
                _ = vacuum_timer.tick(), if vacuum_enabled => {
                    if let Err(err) = self.handle_cluster_vacuum().await {
                        error!("error during cluster vacuum: {:?}", err);
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("application processor shutting down");
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub async fn write_sm_update(
        &self,
        cached_state_changes: &mut Vec<StateChange>,
        application_events_cursor: &mut Option<Vec<u8>>,
        executor_events_cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        debug!("Waking up to process state changes; cached_state_changes={cached_state_changes:?}");
        let metrics_kvs = &[KeyValue::new("op", "get")];
        let _timer_guard = Timer::start_with_labels(&self.write_sm_update_latency, metrics_kvs);

        // 1. First load 100 state changes. Process the `global` state changes first
        // and then the `ns_` state changes
        if cached_state_changes.is_empty() {
            let unprocessed_state_changes = self
                .indexify_state
                .reader()
                .unprocessed_state_changes(executor_events_cursor, application_events_cursor)
                .await?;
            if let Some(cursor) = unprocessed_state_changes.application_state_change_cursor {
                application_events_cursor.replace(cursor);
            };
            if let Some(cursor) = unprocessed_state_changes.executor_state_change_cursor {
                executor_events_cursor.replace(cursor);
            };
            let mut state_changes = unprocessed_state_changes.changes;
            state_changes.reverse();
            cached_state_changes.extend(state_changes);
        }

        self.state_change_queue_depth
            .record(cached_state_changes.len() as u64, &[]);

        // 2. If there are no state changes to process, return
        // and wait for the scheduler to wake us up again when there are state changes
        if cached_state_changes.is_empty() {
            return Ok(());
        }

        // 3. Fire a notification when handling multiple
        // state changes in order to fill in the cache when
        // the next state change is processed.
        //
        // 0 = we fetched all current state changes
        // > 1 = we are processing cached state changes, we need to fetch more when done
        if !cached_state_changes.is_empty() {
            notify.notify_one();
        }

        // 4. Process the next state change from the queue
        let state_change = cached_state_changes.pop().unwrap();
        let state_change_metrics_kvs = &[KeyValue::new(
            "type",
            if state_change.namespace.is_some() {
                "ns"
            } else {
                "global"
            },
        )];
        let _timer_guard =
            Timer::start_with_labels(&self.state_change_latency, state_change_metrics_kvs);
        self.state_changes_total.add(1, state_change_metrics_kvs);
        let sm_update = self.handle_state_change(&state_change).await;

        // 5. Write the state change to the state store
        // If there is an error processing the state change, we write a NOOP state
        // change That way this problematic state change will never be processed
        // again This most likely is a bug but in production we want to move
        // along.
        let sm_update = match sm_update {
            Ok(sm_update) => sm_update,
            Err(err) => {
                // TODO: Determine if error is transient or not to determine if retrying should
                // be done.
                if err.to_string().contains("Operation timed out") {
                    warn!(
                        "transient error processing state change {}, retrying later: {:?}",
                        state_change.change_type, err
                    );
                    cached_state_changes.push(state_change);
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    return Ok(());
                }
                error!(
                    "error processing state change {}, marking as processed: {:?}",
                    state_change.change_type, err
                );

                // Sending NOOP SM Update
                StateMachineUpdateRequest {
                    payload: RequestPayload::ProcessStateChanges(vec![state_change.clone()]),
                }
            }
        };

        // 6. Write the state change
        if let Err(err) = self.indexify_state.write(sm_update).await {
            // TODO: Determine if error is transient or not to determine if retrying should
            // be done.
            error!(
                "error writing state change {}, marking as processed: {:?}",
                state_change.change_type, err,
            );
            // 7. If SM update fails for whatever reason, lets just write a NOOP state
            //    change
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::ProcessStateChanges(vec![state_change.clone()]),
                })
                .await?;
        }

        // Record the state transition duration
        self.state_transition_latency.record(
            get_elapsed_time(state_change.created_at.into(), TimeUnit::Milliseconds),
            state_change_metrics_kvs,
        );
        Ok(())
    }

    #[instrument(skip_all)]
    pub async fn handle_state_change(
        &self,
        state_change: &StateChange,
    ) -> Result<StateMachineUpdateRequest> {
        trace!("processing state change: {}", state_change);
        let kvs = &[KeyValue::new(
            "change_type",
            state_change.change_type.to_string(),
        )];
        let _timer_guard = Timer::start_with_labels(&self.handle_state_change_latency, kvs);
        let indexes = self.indexify_state.in_memory_state.read().await.clone();
        let mut indexes_guard = indexes.write().await;
        let container_scheduler = self.indexify_state.container_scheduler.read().await.clone();
        let mut container_scheduler_guard = container_scheduler.write().await;
        let clock = indexes_guard.clock;

        let task_creator =
            function_run_creator::FunctionRunCreator::new(self.indexify_state.clone(), clock);
        let container_reconciler =
            container_reconciler::ContainerReconciler::new(clock, self.indexify_state.clone());
        let task_allocator = FunctionRunProcessor::new(clock, self.queue_size);
        let sandbox_processor = SandboxProcessor::new(clock);
        let buffer_reconciler = BufferReconciler::new();

        let mut scheduler_update = match &state_change.change_type {
            ChangeType::CreateFunctionCall(req) => {
                let mut scheduler_update = task_creator
                    .handle_blocking_function_call(&mut indexes_guard, req)
                    .await?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();

                scheduler_update.extend(task_allocator.allocate_function_runs(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                )?);
                scheduler_update
            }
            ChangeType::InvokeApplication(ev) => task_allocator.allocate_request(
                &mut indexes_guard,
                &mut container_scheduler_guard,
                &ev.namespace,
                &ev.application,
                &ev.request_id,
                &self.allocate_function_runs_latency,
            )?,
            ChangeType::AllocationOutputsIngested(req) => {
                let mut scheduler_update = task_creator
                    .handle_allocation_ingestion(
                        &mut indexes_guard,
                        &mut container_scheduler_guard,
                        req,
                    )
                    .await?;
                let unallocated_function_runs = indexes_guard.unallocated_function_runs();
                scheduler_update.extend(task_allocator.allocate_function_runs(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                )?);
                scheduler_update
            }
            ChangeType::ExecutorUpserted(ev) => {
                let mut scheduler_update = container_reconciler
                    .reconcile_executor_state(
                        &mut indexes_guard,
                        &mut container_scheduler_guard,
                        &ev.executor_id,
                    )
                    .await?;
                let unallocated_function_runs = indexes_guard.unallocated_function_runs();
                scheduler_update.extend(task_allocator.allocate_function_runs(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                )?);

                scheduler_update.extend(
                    sandbox_processor
                        .allocate_sandboxes(&mut indexes_guard, &mut container_scheduler_guard)?,
                );
                scheduler_update
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut scheduler_update = container_reconciler.deregister_executor(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    &ev.executor_id,
                )?;
                let unallocated_function_runs = scheduler_update.unallocated_function_runs();
                scheduler_update.extend(task_allocator.allocate_function_runs(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                )?);
                scheduler_update
            }
            ChangeType::TombstoneApplication(request) => {
                return Ok(StateMachineUpdateRequest {
                    payload: RequestPayload::DeleteApplicationRequest((
                        DeleteApplicationRequest {
                            namespace: request.namespace.clone(),
                            name: request.application.clone(),
                        },
                        vec![state_change.clone()],
                    )),
                });
            }
            ChangeType::TombstoneRequest(request) => {
                return Ok(StateMachineUpdateRequest {
                    payload: RequestPayload::DeleteRequestRequest((
                        DeleteRequestRequest {
                            namespace: request.namespace.clone(),
                            application: request.application.clone(),
                            request_id: request.request_id.clone(),
                        },
                        vec![state_change.clone()],
                    )),
                });
            }
            ChangeType::CreateSandbox(ev) => sandbox_processor.allocate_sandbox_by_key(
                &mut indexes_guard,
                &mut container_scheduler_guard,
                &ev.namespace,
                ev.sandbox_id.get(),
            )?,
            ChangeType::TerminateSandbox(ev) => sandbox_processor.terminate_sandbox(
                &indexes_guard,
                &mut container_scheduler_guard,
                &ev.namespace,
                ev.sandbox_id.get(),
            )?,
            ChangeType::SnapshotSandbox(ev) => {
                info!(
                    namespace = %ev.namespace,
                    sandbox_id = %ev.sandbox_id,
                    snapshot_id = %ev.snapshot_id,
                    "processing SnapshotSandbox event"
                );
                SchedulerUpdateRequest::default()
            }
            ChangeType::CreateContainerPool(ev) => {
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing CreateContainerPool event"
                );
                SchedulerUpdateRequest::default()
            }
            ChangeType::UpdateContainerPool(ev) => {
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing UpdateContainerPool event"
                );
                SchedulerUpdateRequest::default()
            }
            ChangeType::DeleteContainerPool(ev) => {
                // Container termination is handled by container_scheduler.update()
                // when the DeleteContainerPool payload is applied
                tracing::info!(
                    namespace = %ev.namespace,
                    pool_id = %ev.pool_id,
                    "processing DeleteContainerPool event, issuing storage deletion"
                );
                return Ok(StateMachineUpdateRequest {
                    payload: RequestPayload::DeleteContainerPool((
                        DeleteContainerPoolRequest {
                            namespace: ev.namespace.clone(),
                            pool_id: ev.pool_id.clone(),
                        },
                        vec![state_change.clone()],
                    )),
                });
            }
            ChangeType::DataplaneResultsIngested(ev) => {
                info!(
                    executor_id = %ev.executor_id,
                    num_allocations = ev.allocation_events.len(),
                    num_container_updates = ev.container_state_updates.len(),
                    "processing DataplaneResultsIngested event"
                );
                let mut scheduler_update = SchedulerUpdateRequest::default();

                // Step 1: Terminate containers (lifecycle only).
                // Marks containers as terminated and removes them from the
                // scheduler so Step 3 won't schedule to dead containers.
                //
                // Allocation failure is NOT handled here — the dataplane
                // sends individual AllocationFailed messages with the correct
                // blame-resolved failure reasons, and those are processed in
                // Step 2 via handle_allocation_ingestion.
                for csu in &ev.container_state_updates {
                    let termination_reason = csu
                        .termination_reason
                        .unwrap_or(data_model::ContainerTerminationReason::Unknown);
                    let mut container_update = SchedulerUpdateRequest::default();

                    // Mark the container itself as terminated in the scheduler
                    // so it is excluded from the executor's desired state. This
                    // wakes up the sandbox/allocation stream loops to send a
                    // removal notification to the dataplane.
                    let term_info = container_scheduler_guard
                        .function_containers
                        .get(&csu.container_id)
                        .map(|existing_meta| {
                            let mut fc = existing_meta.function_container.clone();
                            let terminated_state = data_model::ContainerState::Terminated {
                                reason: termination_reason,
                            };
                            // Set terminated state on the container so
                            // terminate_sandbox_for_container reads the correct
                            // termination reason for the sandbox outcome.
                            fc.state = terminated_state.clone();
                            (
                                fc.clone(),
                                data_model::ContainerServerMetadata::new(
                                    ev.executor_id.clone(),
                                    fc,
                                    terminated_state,
                                ),
                            )
                        });
                    if let Some((fc, terminated_meta)) = term_info {
                        container_update
                            .containers
                            .insert(csu.container_id.clone(), Box::new(terminated_meta));

                        // Remove container from executor metadata so
                        // free_resources are updated correctly.
                        if let Some(exec_state) = container_scheduler_guard
                            .executor_states
                            .get(&ev.executor_id)
                        {
                            let mut updated_exec = (**exec_state).clone();
                            if updated_exec.remove_container(&fc).is_ok() {
                                container_update
                                    .updated_executor_states
                                    .insert(ev.executor_id.clone(), Box::new(updated_exec));
                            }
                        }

                        // Terminate the associated sandbox (if any) so it
                        // transitions to Terminated in the state store.
                        let sandbox_update = container_reconciler
                            .terminate_sandbox_for_container(&indexes_guard, &fc)?;
                        container_update.extend(sandbox_update);
                    }

                    // Apply incremental updates so subsequent containers/allocations
                    // see the updated state.
                    let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                        container_update.clone(),
                    ));
                    container_scheduler_guard.update(&payload)?;
                    indexes_guard.update_state(
                        clock,
                        &payload,
                        "dataplane_results_container_term",
                    )?;
                    scheduler_update.extend(container_update);
                }

                // Step 1b: Promote containers and sandboxes for started containers.
                // When the dataplane reports ContainerStarted, update the
                // container state from Pending to Running and promote the
                // associated sandbox (if any) as well.
                for container_id in &ev.container_started_ids {
                    let promote_update = container_reconciler
                        .promote_sandbox_for_started_container(
                            &indexes_guard,
                            &container_scheduler_guard,
                            container_id,
                        )?;
                    if !promote_update.updated_sandboxes.is_empty()
                        || !promote_update.containers.is_empty()
                    {
                        let payload = RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(
                            promote_update.clone(),
                        ));
                        container_scheduler_guard.update(&payload)?;
                        indexes_guard.update_state(
                            clock,
                            &payload,
                            "dataplane_results_container_started",
                        )?;
                        scheduler_update.extend(promote_update);
                    }
                }

                // Step 2: Process allocation results.
                for alloc_event in &ev.allocation_events {
                    let alloc_update = task_creator
                        .handle_allocation_ingestion(
                            &mut indexes_guard,
                            &mut container_scheduler_guard,
                            alloc_event,
                        )
                        .await?;

                    // handle_allocation_ingestion already applies its updates to
                    // in_memory_state and container_scheduler internally, so we
                    // only need to accumulate the result for the final
                    // scheduler_update.  (Calling update_state again here would
                    // be safe — it is idempotent — but redundant.)
                    scheduler_update.extend(alloc_update);
                }

                // Step 3: Schedule pending function runs.
                // Dead containers are already removed from the scheduler,
                // so no rescheduling to dead containers (fixes the hot retry loop).
                let unallocated_function_runs = indexes_guard.unallocated_function_runs();
                info!(
                    num_unallocated = unallocated_function_runs.len(),
                    unallocated_fns = ?unallocated_function_runs.iter().map(|r| format!("{}:{}", r.name, r.id)).collect::<Vec<_>>(),
                    num_executors = container_scheduler_guard.executors.len(),
                    num_containers = container_scheduler_guard.function_containers.len(),
                    "DataplaneResultsIngested Step 3: scheduling pending function runs"
                );
                let alloc_result = task_allocator.allocate_function_runs(
                    &mut indexes_guard,
                    &mut container_scheduler_guard,
                    unallocated_function_runs,
                    &self.allocate_function_runs_latency,
                )?;
                info!(
                    new_allocations = alloc_result.new_allocations.len(),
                    updated_runs = alloc_result.updated_function_runs.len(),
                    new_containers = alloc_result.containers.len(),
                    "DataplaneResultsIngested Step 3: allocation results"
                );
                scheduler_update.extend(alloc_result);

                // Step 4: Schedule pending sandboxes.
                // Container terminations in Step 1 free resources that may
                // unblock sandboxes stuck in Pending/NoResourcesAvailable.
                scheduler_update.extend(
                    sandbox_processor
                        .allocate_sandboxes(&mut indexes_guard, &mut container_scheduler_guard)?,
                );

                scheduler_update
            }
        };

        // Run buffer reconciliation to maintain warm container counts
        let buffer_update =
            buffer_reconciler.reconcile(&indexes_guard, &mut container_scheduler_guard)?;
        scheduler_update.extend(buffer_update);

        Ok(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                update: Box::new(scheduler_update),
                processed_state_changes: vec![state_change.clone()],
            }),
        })
    }

    #[instrument(skip(self))]
    async fn handle_cluster_vacuum(&self) -> Result<()> {
        let container_scheduler = self.indexify_state.container_scheduler.read().await.clone();
        let mut container_scheduler_guard = container_scheduler.write().await;

        let scheduler_update =
            container_scheduler_guard.periodic_vacuum(self.cluster_vacuum_max_idle_age)?;

        if !scheduler_update.containers.is_empty() {
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                        update: Box::new(scheduler_update),
                        processed_state_changes: vec![],
                    }),
                })
                .await?;
        }

        // Fail snapshots that have been stuck in InProgress for too long
        self.handle_snapshot_vacuum().await?;

        Ok(())
    }

    /// Detect and fail snapshots stuck in `InProgress` beyond the configured
    /// timeout. This prevents sandboxes from being permanently stuck in the
    /// `Snapshotting` state if a dataplane crashes mid-snapshot.
    async fn handle_snapshot_vacuum(&self) -> Result<()> {
        if self.snapshot_timeout.is_zero() {
            return Ok(());
        }

        let now_ns = get_epoch_time_in_ns();
        let timeout_ns = self.snapshot_timeout.as_nanos();

        let stale_snapshot_ids: Vec<_> = {
            let in_memory = self.indexify_state.in_memory_state.read().await;
            in_memory
                .snapshots
                .values()
                .filter(|s| {
                    matches!(s.status, SnapshotStatus::InProgress) &&
                        now_ns.saturating_sub(s.creation_time_ns) > timeout_ns
                })
                .map(|s| s.id.clone())
                .collect()
        };

        for snapshot_id in stale_snapshot_ids {
            warn!(
                snapshot_id = snapshot_id.get(),
                timeout_secs = self.snapshot_timeout.as_secs(),
                "Failing stale snapshot stuck in InProgress"
            );
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FailSnapshot(FailSnapshotRequest {
                        snapshot_id,
                        error: "Snapshot timed out".to_string(),
                    }),
                })
                .await?;
        }

        Ok(())
    }
}
