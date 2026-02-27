//! Container lifecycle management for the AllocationController.
//!
//! Handles container startup, health checking, and termination.

use std::time::{Duration, Instant};

use anyhow::Result;
use proto_api::executor_api_pb::{
    AllocationFailureReason,
    ContainerDescription,
    ContainerTerminationReason,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, warn};

use super::{
    AllocationController,
    events::ACEvent,
    types::{ContainerState, FELogCtx, ManagedFE},
};
use crate::{
    driver::{ProcessConfig, ProcessHandle, ProcessType},
    function_executor::{
        controller::FESpawnConfig,
        fe_client::FunctionExecutorGrpcClient,
        health_checker,
    },
    state_file::PersistedContainer,
};

/// Timeout for connecting to the FE after spawning the process.
const FE_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Typed error for FE initialization timeout.
#[derive(Debug)]
struct InitTimedOut(u64);

impl std::fmt::Display for InitTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FE initialization timed out after {}ms", self.0)
    }
}

impl std::error::Error for InitTimedOut {}

impl AllocationController {
    /// Reconcile containers using delta semantics.
    ///
    /// Creates containers in `added_or_updated_fes` that aren't already
    /// tracked, and removes containers whose IDs are in `removed_fe_ids`.
    /// Each command stream message delivers one AddContainer or
    /// RemoveContainer at a time — there is no full-state orphan removal.
    pub(super) async fn reconcile_containers(
        &mut self,
        added_or_updated_fes: Vec<ContainerDescription>,
        removed_fe_ids: Vec<String>,
    ) {
        let mut changed = false;

        // Remove explicitly listed containers.
        for id in &removed_fe_ids {
            if self.containers.contains_key(id) {
                if let Some(fe) = self.containers.get(id) {
                    let ctx = FELogCtx::from_description(&fe.description);
                    info!(
                        namespace = %ctx.namespace,
                        app = %ctx.app,
                        version = %ctx.version,
                        "fn" = %ctx.fn_name,
                        container_id = %ctx.container_id,
                        sandbox_id = %ctx.sandbox_id,
                        pool_id = %ctx.pool_id,
                        current_state = %fe.state,
                        "Container removed from desired set, initiating removal"
                    );
                }
                self.remove_container(id);
                changed = true;
            }
        }

        // Create containers for new FEs (skip already tracked).
        for description in &added_or_updated_fes {
            let id = match &description.id {
                Some(id) => id,
                None => continue,
            };
            if !self.containers.contains_key(id) {
                let ctx = FELogCtx::from_description(description);
                info!(
                    namespace = %ctx.namespace,
                    app = %ctx.app,
                    version = %ctx.version,
                    "fn" = %ctx.fn_name,
                    container_id = %ctx.container_id,
                    sandbox_id = %ctx.sandbox_id,
                    pool_id = %ctx.pool_id,
                    "New container requested, creating"
                );
                self.create_container(description.clone());
                changed = true;
            }
        }

        if changed {
            self.broadcast_state();
        }
    }

    /// Remove a container and clean up all its allocations.
    fn remove_container(&mut self, fe_id: &str) {
        let Some(fe) = self.containers.get(fe_id) else {
            return;
        };

        // Extract info needed before modifying anything
        let (is_starting, is_running, is_terminated) = match &fe.state {
            ContainerState::Starting => (true, false, false),
            ContainerState::Running { .. } => (false, true, false),
            ContainerState::Terminated { .. } => (false, false, true),
        };

        if is_terminated {
            // Already terminated — remove from map
            self.containers.remove(fe_id);
            self.waiting_queue.remove(fe_id);
            self.running_count.remove(fe_id);
            return;
        }

        let remove_ctx = self
            .containers
            .get(fe_id)
            .map(|fe| FELogCtx::from_description(&fe.description));

        if is_running {
            let fe = self.containers.get(fe_id).unwrap();
            if let ContainerState::Running {
                handle,
                health_checker_cancel,
                ..
            } = &fe.state
            {
                info!(
                    container_id = %fe_id,
                    namespace = %remove_ctx.as_ref().map(|c| c.namespace.as_str()).unwrap_or(""),
                    app = %remove_ctx.as_ref().map(|c| c.app.as_str()).unwrap_or(""),
                    "fn" = %remove_ctx.as_ref().map(|c| c.fn_name.as_str()).unwrap_or(""),
                    sandbox_id = %remove_ctx.as_ref().map(|c| c.sandbox_id.as_str()).unwrap_or(""),
                    pool_id = %remove_ctx.as_ref().map(|c| c.pool_id.as_str()).unwrap_or(""),
                    "Removing Running container"
                );
                health_checker_cancel.cancel();
                self.kill_process_fire_and_forget(handle.clone());
            }
            // Remove from state file
            let state_file = self.state_file.clone();
            let id = fe_id.to_string();
            tokio::spawn(async move {
                let _ = state_file.remove(&id).await;
            });
        } else if is_starting {
            info!(
                container_id = %fe_id,
                namespace = %remove_ctx.as_ref().map(|c| c.namespace.as_str()).unwrap_or(""),
                app = %remove_ctx.as_ref().map(|c| c.app.as_str()).unwrap_or(""),
                "fn" = %remove_ctx.as_ref().map(|c| c.fn_name.as_str()).unwrap_or(""),
                sandbox_id = %remove_ctx.as_ref().map(|c| c.sandbox_id.as_str()).unwrap_or(""),
                pool_id = %remove_ctx.as_ref().map(|c| c.pool_id.as_str()).unwrap_or(""),
                "Removing Starting container, marking Terminated"
            );
        }

        // Transition to Terminated
        let reason = ContainerTerminationReason::FunctionCancelled;
        let fe = self.containers.get_mut(fe_id).unwrap();
        fe.state = ContainerState::Terminated { reason };

        // Fail all allocations for this FE (no blame — container removed from desired
        // set)
        self.fail_allocations_for_fe(fe_id, AllocationFailureReason::ContainerTerminated);

        // Return GPUs
        let gpu_allocator = self.config.gpu_allocator.clone();
        let fe = self.containers.get_mut(fe_id).unwrap();
        Self::return_gpus(&gpu_allocator, &mut fe.allocated_gpu_uuids);

        self.config
            .metrics
            .up_down_counters
            .containers_count
            .add(-1, &[]);

        // Notify the server so it can free resources and schedule new containers.
        // Without this, vacuumed containers keep consuming resource slots on the
        // server side, blocking creation of containers for other functions.
        self.send_container_terminated(fe_id, reason);
    }

    /// Create a new container by spawning the startup task.
    fn create_container(&mut self, description: ContainerDescription) {
        let fe_id = description.id.clone().unwrap_or_default();
        let max_concurrency = description.max_concurrency.unwrap_or(1);

        // Allocate GPUs synchronously (no race — single-threaded)
        let gpu_count = description
            .resources
            .as_ref()
            .and_then(|r| r.gpu.as_ref())
            .and_then(|g| g.count)
            .unwrap_or(0);

        let create_ctx = FELogCtx::from_description(&description);

        let allocated_gpu_uuids = if gpu_count > 0 {
            match self.config.gpu_allocator.allocate(gpu_count) {
                Ok(uuids) => uuids,
                Err(e) => {
                    warn!(
                        container_id = %fe_id,
                        namespace = %create_ctx.namespace,
                        app = %create_ctx.app,
                        "fn" = %create_ctx.fn_name,
                        sandbox_id = %create_ctx.sandbox_id,
                        pool_id = %create_ctx.pool_id,
                        gpu_count = gpu_count,
                        error = %e,
                        "GPU allocation failed"
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };

        info!(
            container_id = %fe_id,
            namespace = %create_ctx.namespace,
            app = %create_ctx.app,
            "fn" = %create_ctx.fn_name,
            sandbox_id = %create_ctx.sandbox_id,
            pool_id = %create_ctx.pool_id,
            max_concurrency = max_concurrency,
            gpus = ?allocated_gpu_uuids,
            "Creating container"
        );

        self.config
            .metrics
            .counters
            .function_executor_creates
            .add(1, &[]);
        self.config
            .metrics
            .up_down_counters
            .containers_count
            .add(1, &[]);

        let managed = ManagedFE {
            description: description.clone(),
            state: ContainerState::Starting,
            max_concurrency,
            allocated_gpu_uuids: allocated_gpu_uuids.clone(),
            created_at: Instant::now(),
        };
        self.containers.insert(fe_id.clone(), managed);

        // Spawn startup task with panic-safety wrapper
        let event_tx = self.event_tx.clone();
        let config = self.config.clone();
        let gpu_uuids_for_task = allocated_gpu_uuids;
        let fe_id_for_task = fe_id.clone();
        let desc_for_task = description.clone();

        let func_ref = description.function.as_ref();
        let namespace = func_ref
            .and_then(|f| f.namespace.as_deref())
            .unwrap_or("")
            .to_string();
        let app = func_ref
            .and_then(|f| f.application_name.as_deref())
            .unwrap_or("")
            .to_string();
        let fn_name = func_ref
            .and_then(|f| f.function_name.as_deref())
            .unwrap_or("")
            .to_string();
        let version = func_ref
            .and_then(|f| f.application_version.as_deref())
            .unwrap_or("")
            .to_string();
        let executor_id = config.executor_id.clone();

        let panic_fe_id = fe_id.clone();
        let startup_sandbox_id = description
            .sandbox_metadata
            .as_ref()
            .and_then(|m| m.sandbox_id.as_deref())
            .unwrap_or("")
            .to_string();
        let startup_pool_id = description.pool_id.as_deref().unwrap_or("").to_string();
        let panic_sandbox_id = startup_sandbox_id.clone();
        let panic_pool_id = startup_pool_id.clone();
        tokio::spawn(async move {
            let result = tokio::spawn(
                start_fe_process_and_initialize(config, desc_for_task, gpu_uuids_for_task)
                    .instrument(tracing::info_span!(
                        "fe_startup",
                        container_id = %fe_id_for_task,
                        executor_id = %executor_id,
                        namespace = %namespace,
                        app = %app,
                        "fn" = %fn_name,
                        version = %version,
                        sandbox_id = %startup_sandbox_id,
                        pool_id = %startup_pool_id,
                    )),
            )
            .await;

            let event = match result {
                Ok(inner_result) => ACEvent::ContainerStartupComplete {
                    fe_id: fe_id_for_task,
                    result: inner_result,
                },
                Err(join_err) => {
                    // Panic or cancellation — send failure
                    error!(container_id = %panic_fe_id, sandbox_id = %panic_sandbox_id, pool_id = %panic_pool_id, error = %join_err, "Container startup task panicked");
                    ACEvent::ContainerStartupComplete {
                        fe_id: fe_id_for_task,
                        result: Err(anyhow::anyhow!("Startup task panicked: {}", join_err)),
                    }
                }
            };
            let _ = event_tx.send(event);
        });
    }

    /// Handle the result of a container startup task.
    pub(super) async fn handle_container_startup_complete(
        &mut self,
        fe_id: String,
        result: Result<(ProcessHandle, FunctionExecutorGrpcClient)>,
    ) {
        let Some(fe) = self.containers.get_mut(&fe_id) else {
            // FE was removed while startup was in progress — kill the handle
            if let Ok((handle, _)) = result {
                warn!(container_id = %fe_id, "Startup completed but FE no longer tracked, killing handle");
                self.kill_process_fire_and_forget(handle);
            }
            return;
        };

        let startup_ctx = FELogCtx::from_description(&fe.description);

        match &fe.state {
            ContainerState::Terminated { .. } => {
                // FE was removed during startup — kill the returned handle
                // This is the KEY FIX for orphaned Docker containers.
                if let Ok((handle, _)) = result {
                    warn!(
                        container_id = %fe_id,
                        namespace = %startup_ctx.namespace,
                        app = %startup_ctx.app,
                        "fn" = %startup_ctx.fn_name,
                        sandbox_id = %startup_ctx.sandbox_id,
                        pool_id = %startup_ctx.pool_id,
                        "Startup completed but FE is Terminated, killing handle"
                    );
                    self.kill_process_fire_and_forget(handle);
                }
                return;
            }
            ContainerState::Running { .. } => {
                // Already running (duplicate event?) — kill if we got a new handle
                if let Ok((handle, _)) = result {
                    warn!(
                        container_id = %fe_id,
                        namespace = %startup_ctx.namespace,
                        app = %startup_ctx.app,
                        "fn" = %startup_ctx.fn_name,
                        sandbox_id = %startup_ctx.sandbox_id,
                        pool_id = %startup_ctx.pool_id,
                        "Startup completed but FE is already Running, killing duplicate"
                    );
                    self.kill_process_fire_and_forget(handle);
                }
                return;
            }
            ContainerState::Starting => {
                // Expected state — proceed with transition
            }
        }

        let create_start = fe.created_at;

        match result {
            Ok((handle, client)) => {
                self.config
                    .metrics
                    .histograms
                    .function_executor_create_latency_seconds
                    .record(create_start.elapsed().as_secs_f64(), &[]);

                let ctx = FELogCtx::from_description(&fe.description);
                info!(
                    namespace = %ctx.namespace,
                    app = %ctx.app,
                    version = %ctx.version,
                    "fn" = %ctx.fn_name,
                    container_id = %fe_id,
                    sandbox_id = %ctx.sandbox_id,
                    pool_id = %ctx.pool_id,
                    latency_ms = %create_start.elapsed().as_millis(),
                    "Container started successfully: Starting -> Running"
                );

                // Spawn health checker with panic-safety wrapper
                let health_cancel = CancellationToken::new();
                let health_cancel_clone = health_cancel.clone();
                let event_tx = self.event_tx.clone();
                let panic_event_tx = event_tx.clone();
                let health_client = client.clone();
                let health_driver = self.config.driver.clone();
                let health_handle = handle.clone();
                let health_fe_id = fe_id.clone();
                let panic_fe_id = fe_id.clone();
                let health_metrics = self.config.metrics.clone();

                tokio::spawn(async move {
                    let result = tokio::spawn(async move {
                        if let Some(reason) = health_checker::run_health_check_loop(
                            health_client,
                            health_driver,
                            health_handle,
                            health_cancel_clone,
                            &health_fe_id,
                            health_metrics,
                        )
                        .await
                        {
                            let _ = event_tx.send(ACEvent::ContainerTerminated {
                                fe_id: health_fe_id,
                                reason,
                                blamed_allocation_id: None,
                            });
                        }
                    })
                    .await;

                    if let Err(join_err) = result {
                        // Panic safety — notify controller via the cloned sender
                        error!(container_id = %panic_fe_id, error = %join_err, "Health checker panicked");
                        let _ = panic_event_tx.send(ACEvent::ContainerTerminated {
                            fe_id: panic_fe_id,
                            reason: ContainerTerminationReason::Unknown,
                            blamed_allocation_id: None,
                        });
                    }
                });

                let fe = self.containers.get_mut(&fe_id).unwrap();
                fe.state = ContainerState::Running {
                    handle: handle.clone(),
                    client: Box::new(client),
                    health_checker_cancel: health_cancel,
                };

                // Persist to state file for recovery after restart
                let persisted = PersistedContainer {
                    container_id: fe_id.clone(),
                    handle_id: handle.id.clone(),
                    daemon_addr: handle.daemon_addr.clone().unwrap_or_default(),
                    http_addr: handle.http_addr.clone().unwrap_or_default(),
                    container_ip: handle.container_ip.clone(),
                    started_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    description_proto: Some(PersistedContainer::encode_description(
                        &fe.description,
                    )),
                };
                let state_file = self.state_file.clone();
                let persist_fe_id = fe_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = state_file.upsert(persisted).await {
                        tracing::warn!(container_id = %persist_fe_id, error = ?e, "Failed to persist AC container state");
                    }
                });

                // Notify the server so it can update container state.
                let response =
                    crate::function_executor::proto_convert::make_container_started_response(
                        &fe_id,
                    );
                let _ = self.config.container_state_tx.send(response);

                // Unblock WaitingForContainer allocations
                self.try_schedule();
                self.broadcast_state();
            }
            Err(e) => {
                let ctx = FELogCtx::from_description(&fe.description);
                error!(
                    namespace = %ctx.namespace,
                    app = %ctx.app,
                    version = %ctx.version,
                    "fn" = %ctx.fn_name,
                    container_id = %fe_id,
                    sandbox_id = %ctx.sandbox_id,
                    pool_id = %ctx.pool_id,
                    error = ?e,
                    "Container startup failed: Starting -> Terminated"
                );
                self.config
                    .metrics
                    .counters
                    .function_executor_create_errors
                    .add(1, &[]);
                self.config
                    .metrics
                    .histograms
                    .function_executor_create_latency_seconds
                    .record(create_start.elapsed().as_secs_f64(), &[]);
                self.config
                    .metrics
                    .up_down_counters
                    .containers_count
                    .add(-1, &[]);

                // Determine termination reason from error
                let reason = if e.downcast_ref::<crate::driver::ImageError>().is_some() {
                    ContainerTerminationReason::StartupFailedBadImage
                } else if e.is::<InitTimedOut>() {
                    ContainerTerminationReason::StartupFailedFunctionTimeout
                } else {
                    ContainerTerminationReason::StartupFailedInternalError
                };

                let fe = self.containers.get_mut(&fe_id).unwrap();
                fe.state = ContainerState::Terminated { reason };

                // Fail all allocations — all blamed since container couldn't start.
                let failure_reason =
                    crate::function_executor::proto_convert::termination_to_failure_reason(reason);
                self.fail_allocations_for_fe(&fe_id, failure_reason);

                // Return GPUs
                let gpu_allocator = self.config.gpu_allocator.clone();
                let fe = self.containers.get_mut(&fe_id).unwrap();
                Self::return_gpus(&gpu_allocator, &mut fe.allocated_gpu_uuids);

                // Notify result pipeline so container termination is merged with
                // allocation results in the next report_results RPC.
                self.send_container_terminated(&fe_id, reason);

                self.broadcast_state();
            }
        }
    }

    /// Handle container termination (from health checker).
    pub(super) fn handle_container_terminated(
        &mut self,
        fe_id: String,
        reason: ContainerTerminationReason,
        blamed_allocation_id: Option<String>,
    ) {
        let Some(fe) = self.containers.get_mut(&fe_id) else {
            return;
        };

        // Idempotent: already Terminated → ignore
        if matches!(fe.state, ContainerState::Terminated { .. }) {
            return;
        }

        let ctx = FELogCtx::from_description(&fe.description);
        warn!(
            namespace = %ctx.namespace,
            app = %ctx.app,
            version = %ctx.version,
            "fn" = %ctx.fn_name,
            container_id = %fe_id,
            sandbox_id = %ctx.sandbox_id,
            pool_id = %ctx.pool_id,
            from_state = %fe.state,
            reason = ?reason,
            "Container terminated: {} -> Terminated({:?})", fe.state, reason
        );

        // Extract handle for killing (if Running)
        let handle_to_kill = match &fe.state {
            ContainerState::Running {
                handle,
                health_checker_cancel,
                ..
            } => {
                health_checker_cancel.cancel();
                Some(handle.clone())
            }
            _ => None,
        };

        let _ = blamed_allocation_id; // No longer used for per-alloc blame.

        fe.state = ContainerState::Terminated { reason };

        // Fail allocations for this FE. Blamed allocations get the specific
        // failure reason; non-blamed get FunctionExecutorTerminated (free retry).
        let failure_reason =
            crate::function_executor::proto_convert::termination_to_failure_reason(reason);
        self.fail_allocations_for_fe(&fe_id, failure_reason);

        // Kill process & return GPUs
        if let Some(handle) = handle_to_kill {
            self.kill_process_fire_and_forget(handle);
        }
        let gpu_allocator = self.config.gpu_allocator.clone();
        let fe = self.containers.get_mut(&fe_id).unwrap();
        Self::return_gpus(&gpu_allocator, &mut fe.allocated_gpu_uuids);

        // Remove from state file
        let state_file = self.state_file.clone();
        let id = fe_id.clone();
        tokio::spawn(async move {
            let _ = state_file.remove(&id).await;
        });

        self.config
            .metrics
            .up_down_counters
            .containers_count
            .add(-1, &[]);
        self.config
            .metrics
            .counters
            .function_executor_destroys
            .add(1, &[]);

        // Notify result pipeline so container termination is merged with
        // allocation results in the next report_results RPC.
        self.send_container_terminated(&fe_id, reason);

        self.broadcast_state();
    }

    /// Send a `ContainerTerminated` `CommandResponse` to the result pipeline
    /// so it gets sent via `report_command_responses` alongside allocation
    /// results.
    fn send_container_terminated(&self, container_id: &str, reason: ContainerTerminationReason) {
        let response = crate::function_executor::proto_convert::make_container_terminated_response(
            container_id,
            reason,
        );
        let _ = self.config.container_state_tx.send(response);
    }
}

// ---------------------------------------------------------------------------
// Standalone async functions for container startup
// ---------------------------------------------------------------------------

/// Start a function executor process, connect, and initialize it.
///
/// This is the "startup task" spawned by create_container. It runs in a
/// separate tokio task and sends the result back via ACEvent.
async fn start_fe_process_and_initialize(
    config: FESpawnConfig,
    description: ContainerDescription,
    gpu_uuids: Vec<String>,
) -> Result<(ProcessHandle, FunctionExecutorGrpcClient)> {
    let fe_id = description.id.clone().unwrap_or_default();
    let metrics = config.metrics.clone();

    // Start the FE process
    let ctx = FELogCtx::from_description(&description);
    info!(container_id = %fe_id, sandbox_id = %ctx.sandbox_id, pool_id = %ctx.pool_id, "Starting function executor process");
    let start_time = Instant::now();
    let handle = match start_fe_process(&config, &description, &gpu_uuids).await {
        Ok(handle) => handle,
        Err(e) => {
            tracing::error!(error = ?e, %fe_id, sandbox_id = %ctx.sandbox_id, pool_id = %ctx.pool_id, "Failed to start function executor process");
            metrics
                .counters
                .function_executor_create_server_errors
                .add(1, &[]);
            return Err(e);
        }
    };
    metrics
        .histograms
        .function_executor_create_server_latency_seconds
        .record(start_time.elapsed().as_secs_f64(), &[]);

    // Connect and initialize
    match connect_and_initialize(&config, &description, &handle).await {
        Ok(client) => Ok((handle, client)),
        Err(e) => {
            // Kill the process since we can't use it
            let _ = config.driver.kill(&handle).await;
            Err(e)
        }
    }
}

/// Start the FE subprocess using the driver.
async fn start_fe_process(
    config: &FESpawnConfig,
    description: &ContainerDescription,
    gpu_uuids: &[String],
) -> Result<ProcessHandle> {
    let fe_id = description.id.clone().unwrap_or_default();
    let func_ref = description.function.as_ref();
    let namespace = func_ref.and_then(|f| f.namespace.as_deref()).unwrap_or("");
    let app = func_ref
        .and_then(|f| f.application_name.as_deref())
        .unwrap_or("");
    let function = func_ref
        .and_then(|f| f.function_name.as_deref())
        .unwrap_or("");
    let version = func_ref
        .and_then(|f| f.application_version.as_deref())
        .unwrap_or("");

    // Resolve image
    let ctx = FELogCtx::from_description(description);
    tracing::debug!(container_id = %fe_id, %namespace, %app, "fn" = %function, %version, "Resolving function image");
    let image = config
        .image_resolver
        .function_image(namespace, app, function, version)
        .await
        .inspect_err(|err| {
            tracing::error!(container_id = %fe_id, %namespace, %app, "fn" = %function, %version, error = ?err, "Failed to resolve function image");
        })?;

    info!(
        container_id = %fe_id,
        sandbox_id = %ctx.sandbox_id,
        pool_id = %ctx.pool_id,
        namespace = %namespace,
        image = ?image,
        "Image resolved for function container"
    );

    // Build environment variables
    let mut env = vec![
        (
            "INDEXIFY_EXECUTOR_ID".to_string(),
            config.executor_id.clone(),
        ),
        ("INDEXIFY_FE_ID".to_string(), fe_id.clone()),
    ];

    // Fetch and inject secrets
    let secrets = config
        .secrets_provider
        .fetch_secrets(&config.executor_id, namespace, &description.secret_names)
        .await?;
    for (k, v) in secrets {
        env.push((k, v));
    }

    let sandbox_id = description
        .sandbox_metadata
        .as_ref()
        .and_then(|m| m.sandbox_id.as_deref())
        .unwrap_or("")
        .to_string();
    let pool_id = description.pool_id.as_deref().unwrap_or("").to_string();

    let labels = vec![
        ("indexify.container_id".to_string(), fe_id.clone()),
        ("indexify.namespace".to_string(), namespace.to_string()),
        ("indexify.application".to_string(), app.to_string()),
        ("indexify.function".to_string(), function.to_string()),
        ("indexify.sandbox_id".to_string(), sandbox_id),
        ("indexify.pool_id".to_string(), pool_id),
    ];

    let process_config = ProcessConfig {
        id: fe_id.clone(),
        process_type: ProcessType::Function,
        image,
        command: config.fe_binary_path.clone(),
        args: vec![
            format!("--executor-id={}", config.executor_id),
            format!("--function-executor-id={}", fe_id),
        ],
        env,
        working_dir: None,
        resources: {
            let gpu_device_ids = if !gpu_uuids.is_empty() {
                Some(gpu_uuids.to_vec())
            } else {
                None
            };
            description
                .resources
                .as_ref()
                .map(|r| crate::driver::ResourceLimits {
                    cpu_millicores: r.cpu_ms_per_sec.map(|v| v as u64),
                    memory_bytes: r.memory_bytes,
                    gpu_device_ids,
                })
        },
        labels,
        rootfs_overlay: None,
    };

    config.driver.start(process_config).await
}

/// Connect to the FE gRPC server and initialize it.
async fn connect_and_initialize(
    config: &FESpawnConfig,
    description: &ContainerDescription,
    handle: &ProcessHandle,
) -> Result<FunctionExecutorGrpcClient> {
    let mut client = connect_to_fe(config, handle).await?;
    initialize_fe(config, description, &mut client).await?;
    Ok(client)
}

/// Connect to the FE gRPC server with retry.
async fn connect_to_fe(
    config: &FESpawnConfig,
    handle: &ProcessHandle,
) -> Result<FunctionExecutorGrpcClient> {
    let addr = handle
        .daemon_addr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No FE gRPC address"))?;

    info!(addr = %addr, "Connecting to function executor");

    let driver = config.driver.clone();
    let process_handle = Some(handle.clone());
    let addr_owned = addr.to_string();

    let connect_start = Instant::now();
    let connect_result = crate::retry::retry_until_deadline(
        FE_READY_TIMEOUT,
        Duration::from_millis(100),
        &format!("connecting to function executor at {}", addr),
        || FunctionExecutorGrpcClient::connect(&addr_owned),
        || {
            let driver = driver.clone();
            let process_handle = process_handle.clone();
            async move {
                if let Some(h) = &process_handle &&
                    !driver.alive(h).await.unwrap_or(false)
                {
                    let exit_status = driver.get_exit_status(h).await.ok().flatten();
                    anyhow::bail!(
                        "Function executor process died before accepting connections \
                         (exit_status={:?})",
                        exit_status,
                    );
                }
                Ok(())
            }
        },
    )
    .await;
    config
        .metrics
        .histograms
        .function_executor_establish_channel_latency_seconds
        .record(connect_start.elapsed().as_secs_f64(), &[]);
    if connect_result.is_err() {
        config
            .metrics
            .counters
            .function_executor_establish_channel_errors
            .add(1, &[]);
    }
    let mut client = connect_result?;

    // Verify connectivity
    let info_start = Instant::now();
    let info_result = client.get_info().await;
    config
        .metrics
        .histograms
        .function_executor_get_info_rpc_latency_seconds
        .record(info_start.elapsed().as_secs_f64(), &[]);
    match &info_result {
        Ok(info) => {
            config.metrics.counters.record_function_executor_info(
                info.version.as_deref().unwrap_or(""),
                info.sdk_version.as_deref().unwrap_or(""),
                info.sdk_language.as_deref().unwrap_or(""),
                info.sdk_language_version.as_deref().unwrap_or(""),
            );
        }
        Err(_) => {
            config
                .metrics
                .counters
                .function_executor_get_info_rpc_errors
                .add(1, &[]);
        }
    }
    let info = info_result?;
    info!(
        version = ?info.version,
        sdk_version = ?info.sdk_version,
        sdk_language = ?info.sdk_language,
        "Connected to function executor"
    );

    Ok(client)
}

/// Download application code and send initialization RPC.
async fn initialize_fe(
    config: &FESpawnConfig,
    description: &ContainerDescription,
    client: &mut FunctionExecutorGrpcClient,
) -> Result<()> {
    let application_code = download_app_code(config, description).await?;

    let init_request = proto_api::function_executor_pb::InitializeRequest {
        function: description.function.as_ref().map(|f| {
            proto_api::function_executor_pb::FunctionRef {
                namespace: f.namespace.clone(),
                application_name: f.application_name.clone(),
                function_name: f.function_name.clone(),
                application_version: f.application_version.clone(),
            }
        }),
        application_code,
    };

    let timeout_ms = description.initialization_timeout_ms.unwrap_or(0);
    let init_future = client.initialize(init_request);

    let init_start = Instant::now();
    let init_result: Result<_> = if timeout_ms > 0 {
        match tokio::time::timeout(Duration::from_millis(timeout_ms as u64), init_future).await {
            Err(_) => Err(anyhow::Error::from(InitTimedOut(timeout_ms as u64))),
            Ok(Err(e)) => {
                error!(error = ?e, "FE initialize RPC call failed");
                Err(e)
            }
            Ok(Ok(resp)) => Ok(resp),
        }
    } else {
        init_future.await.map_err(|e| {
            error!(error = ?e, "FE initialize RPC call failed");
            e
        })
    };
    config
        .metrics
        .histograms
        .function_executor_initialize_rpc_latency_seconds
        .record(init_start.elapsed().as_secs_f64(), &[]);
    if init_result.is_err() {
        config
            .metrics
            .counters
            .function_executor_initialize_rpc_errors
            .add(1, &[]);
    }
    let init_response = init_result?;

    use proto_api::function_executor_pb::InitializationOutcomeCode;
    match init_response.outcome_code() {
        InitializationOutcomeCode::Success => {
            info!("Function executor initialized successfully");
        }
        InitializationOutcomeCode::Failure => {
            anyhow::bail!(
                "FE initialization failed: {:?}",
                init_response.failure_reason
            );
        }
        InitializationOutcomeCode::Unknown => {
            anyhow::bail!("FE initialization returned unknown outcome");
        }
    }

    Ok(())
}

/// Download application code using the code cache.
async fn download_app_code(
    config: &FESpawnConfig,
    description: &ContainerDescription,
) -> Result<Option<proto_api::function_executor_pb::SerializedObject>> {
    let func_ref = description
        .function
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No function reference in description"))?;

    let namespace = func_ref.namespace.as_deref().unwrap_or("");
    let app_name = func_ref.application_name.as_deref().unwrap_or("");
    let app_version = func_ref.application_version.as_deref().unwrap_or("");

    let data_payload = description
        .application
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No application data payload in description"))?;

    let code_uri = data_payload.uri.as_deref().unwrap_or("");

    if code_uri.is_empty() {
        return Ok(None);
    }

    let expected_sha256 = data_payload.sha256_hash.as_deref();
    let code_bytes = config
        .code_cache
        .get_or_download(namespace, app_name, app_version, code_uri, expected_sha256)
        .await?;

    use proto_api::function_executor_pb::SerializedObjectManifest;

    let encoding: i32 = data_payload.encoding.unwrap_or(4); // default BINARY_ZIP = 4

    let manifest = SerializedObjectManifest {
        encoding: Some(encoding),
        encoding_version: Some(data_payload.encoding_version.unwrap_or(0)),
        size: Some(code_bytes.len() as u64),
        metadata_size: Some(data_payload.metadata_size.unwrap_or(0)),
        sha256_hash: data_payload.sha256_hash.clone(),
        content_type: data_payload.content_type.clone(),
        source_function_call_id: None,
    };

    Ok(Some(proto_api::function_executor_pb::SerializedObject {
        manifest: Some(manifest),
        data: Some(code_bytes.to_vec()),
    }))
}
