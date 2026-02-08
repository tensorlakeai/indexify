//! Event-driven state machine for managing a single function executor
//! subprocess.
//!
//! The controller manages the lifecycle of a function executor process:
//! startup → initialization → allocation execution → shutdown.
//!
//! It runs as a tokio task with a select loop processing commands from
//! the StateReconciler and events from background tasks (health checker,
//! allocation runners).

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    AllocationResult as ServerAllocationResult,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{Instrument, error, info, warn};

use super::{
    allocation_finalize,
    allocation_prep,
    events::{
        AllocationOutcome,
        CompletedAllocation,
        FECommand,
        FEEvent,
        FinalizationContext,
        PreparedAllocation,
    },
    fe_client::FunctionExecutorGrpcClient,
    health_checker,
    watcher_registry::WatcherRegistry,
};
use crate::{
    blob_ops::BlobStore,
    code_cache::CodeCache,
    driver::{ProcessConfig, ProcessDriver, ProcessHandle, ProcessType},
    function_container_manager::ImageResolver,
};

/// Timeout for connecting to the FE after spawning the process.
const FE_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Handle returned to the StateReconciler for communicating with a controller.
pub struct FEControllerHandle {
    /// Send commands (new allocations, shutdown).
    pub command_tx: mpsc::UnboundedSender<FECommand>,
    /// Read current FE state for heartbeat reporting.
    pub state_rx: watch::Receiver<FunctionExecutorState>,
}

/// State of an allocation tracked by the controller.
struct AllocationInfo {
    allocation: ServerAllocation,
    state: AllocationState,
    cancel_token: CancellationToken,
    /// Prepared inputs (set after prep completes, taken when execution starts).
    prepared: Option<PreparedAllocation>,
    /// Server-side result (built from execution outcome, sent during
    /// finalization).
    server_result: Option<ServerAllocationResult>,
    /// Context for finalization (accumulated across prep and execution).
    finalization_ctx: Option<FinalizationContext>,
}

#[derive(Debug, Clone, PartialEq)]
enum AllocationState {
    Preparing,
    Runnable,
    Running,
    Finalizing,
    Done,
}

/// The main controller that manages one function executor process.
pub struct FunctionExecutorController {
    description: FunctionExecutorDescription,
    // Process lifecycle
    handle: Option<ProcessHandle>,
    driver: Arc<dyn ProcessDriver>,
    // gRPC client to the FE subprocess
    client: Option<FunctionExecutorGrpcClient>,
    // Inbound events from background tasks
    event_tx: mpsc::UnboundedSender<FEEvent>,
    event_rx: mpsc::UnboundedReceiver<FEEvent>,
    // Inbound commands from StateReconciler
    command_rx: mpsc::UnboundedReceiver<FECommand>,
    // Outbound: completed allocation results → StateReporter
    result_tx: mpsc::UnboundedSender<CompletedAllocation>,
    // Outbound: current FE state for heartbeat reporting
    state_tx: watch::Sender<FunctionExecutorState>,
    // Allocation tracking
    allocations: HashMap<String, AllocationInfo>,
    runnable_queue: VecDeque<String>,
    running_set: HashSet<String>,
    max_concurrency: u32,
    // Cancellation hierarchy
    cancel_token: CancellationToken,
    // Server API client for call_function RPC
    server_channel: Channel,
    // Blob store and code cache
    blob_store: Arc<BlobStore>,
    code_cache: Arc<CodeCache>,
    // Configuration
    executor_id: String,
    fe_binary_path: String,
    // Image resolver for resolving container images
    image_resolver: Arc<dyn ImageResolver>,
    // Watcher registry for function call result routing
    watcher_registry: WatcherRegistry,
    // Token to cancel the health checker when the process is killed
    fe_process_cancel_token: Option<CancellationToken>,
}

impl FunctionExecutorController {
    /// Spawn a new controller as a tokio task. Returns a handle for
    /// communication.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        description: FunctionExecutorDescription,
        driver: Arc<dyn ProcessDriver>,
        image_resolver: Arc<dyn ImageResolver>,
        result_tx: mpsc::UnboundedSender<CompletedAllocation>,
        cancel_token: CancellationToken,
        server_channel: Channel,
        blob_store: Arc<BlobStore>,
        code_cache: Arc<CodeCache>,
        executor_id: String,
        fe_binary_path: String,
        watcher_registry: WatcherRegistry,
    ) -> FEControllerHandle {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let initial_state = FunctionExecutorState {
            description: Some(description.clone()),
            status: Some(FunctionExecutorStatus::Pending.into()),
            termination_reason: None,
            allocation_ids_caused_termination: vec![],
        };

        let (state_tx, state_rx) = watch::channel(initial_state);

        let max_concurrency = description.max_concurrency.unwrap_or(1);
        let fe_id = description.id.clone().unwrap_or_default();

        let mut controller = Self {
            description,
            handle: None,
            driver,
            client: None,
            event_tx,
            event_rx,
            command_rx,
            result_tx,
            state_tx,
            allocations: HashMap::new(),
            runnable_queue: VecDeque::new(),
            running_set: HashSet::new(),
            max_concurrency,
            cancel_token,
            server_channel,
            blob_store,
            code_cache,
            executor_id,
            fe_binary_path,
            image_resolver,
            watcher_registry,
            fe_process_cancel_token: None,
        };

        tokio::spawn(
            async move {
                controller.run().await;
            }
            .instrument(tracing::info_span!("fe_controller", fe_id = %fe_id)),
        );

        FEControllerHandle {
            command_tx,
            state_rx,
        }
    }

    /// Main control loop.
    async fn run(&mut self) {
        let metrics = crate::metrics::DataplaneCounters::new();
        let histograms = crate::metrics::DataplaneHistograms::new();
        let up_down = crate::metrics::DataplaneUpDownCounters::new();

        let create_start = Instant::now();
        metrics.function_executor_creates.add(1, &[]);
        up_down.function_executors_count.add(1, &[]);

        // Phase 1: Start the FE process
        info!(fe_id = ?self.description.id, "Starting function executor");
        let start_process_time = Instant::now();
        if let Err(e) = self.start_fe_process().await {
            error!(error = %e, "Failed to start function executor process");
            metrics.function_executor_create_errors.add(1, &[]);
            metrics.function_executor_create_server_errors.add(1, &[]);
            histograms
                .function_executor_create_server_latency_seconds
                .record(start_process_time.elapsed().as_secs_f64(), &[]);
            histograms
                .function_executor_create_latency_seconds
                .record(create_start.elapsed().as_secs_f64(), &[]);
            up_down.function_executors_count.add(-1, &[]);
            self.transition_to_terminated(
                FunctionExecutorTerminationReason::StartupFailedInternalError,
            );
            return;
        }
        histograms
            .function_executor_create_server_latency_seconds
            .record(start_process_time.elapsed().as_secs_f64(), &[]);

        // Phase 2: Connect to FE and initialize
        match self.connect_and_initialize().await {
            Ok(client) => {
                histograms
                    .function_executor_create_latency_seconds
                    .record(create_start.elapsed().as_secs_f64(), &[]);
                self.client = Some(client.clone());
                self.update_state(FunctionExecutorStatus::Running, None);

                // Create a cancellation token for the process-bound tasks
                let process_token = CancellationToken::new();
                self.fe_process_cancel_token = Some(process_token.clone());

                // Spawn health checker
                let health_cancel = process_token.clone();
                let fe_id = self.description.id.clone().unwrap_or_default();
                let event_tx = self.event_tx.clone();
                tokio::spawn(health_checker::run_health_checker(
                    client,
                    event_tx,
                    health_cancel,
                    fe_id,
                ));
            }
            Err(e) => {
                info!(error = %e, "Failed to connect/initialize function executor");
                metrics.function_executor_create_errors.add(1, &[]);
                histograms
                    .function_executor_create_latency_seconds
                    .record(create_start.elapsed().as_secs_f64(), &[]);
                up_down.function_executors_count.add(-1, &[]);
                // Kill the process since we can't use it
                if let Some(handle) = &self.handle {
                    let _ = self.driver.kill(handle).await;
                }

                let reason = if e.to_string().contains("FE initialization timed out") {
                    FunctionExecutorTerminationReason::StartupFailedFunctionTimeout
                } else {
                    FunctionExecutorTerminationReason::StartupFailedInternalError
                };

                self.transition_to_terminated(reason);

                // Drain command channel to handle any allocations that were sent while we were
                // initializing
                self.command_rx.close();
                while let Some(cmd) = self.command_rx.recv().await {
                    if let FECommand::AddAllocation(alloc) = cmd {
                        self.handle_add_allocation(alloc);
                    }
                }
                return;
            }
        }

        info!(fe_id = ?self.description.id, "Function executor running, entering event loop");

        // Phase 3: Event loop
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    info!("Controller cancelled, shutting down");
                    self.shutdown().await;
                    break;
                }
                Some(cmd) = self.command_rx.recv() => {
                    match cmd {
                        FECommand::AddAllocation(alloc) => self.handle_add_allocation(alloc),
                        FECommand::Shutdown => {
                            self.shutdown().await;
                            break;
                        }
                    }
                }
                Some(event) = self.event_rx.recv() => {
                    let should_exit = self.handle_event(event).await;
                    if should_exit {
                        break;
                    }
                }
            }
        }
    }

    /// Start the FE subprocess using the driver.
    async fn start_fe_process(&mut self) -> Result<()> {
        let fe_id = self.description.id.clone().unwrap_or_default();
        let func_ref = self.description.function.as_ref();
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

        // Resolve image (used by Docker driver, ignored by ForkExec)
        let image = self
            .image_resolver
            .function_image(namespace, app, function, version)
            .ok();

        // Build environment variables
        let env = vec![
            ("INDEXIFY_EXECUTOR_ID".to_string(), self.executor_id.clone()),
            ("INDEXIFY_FE_ID".to_string(), fe_id.clone()),
        ];

        let config =
            ProcessConfig {
                id: fe_id.clone(),
                process_type: ProcessType::Function,
                image,
                command: self.fe_binary_path.clone(),
                args: vec![
                    format!("--executor-id={}", self.executor_id),
                    format!("--function-executor-id={}", fe_id),
                ],
                env,
                working_dir: None,
                resources: self.description.resources.as_ref().map(|r| {
                    crate::driver::ResourceLimits {
                        cpu_millicores: r.cpu_ms_per_sec.map(|v| v as u64),
                        memory_mb: r.memory_bytes.map(|v| v / (1024 * 1024)),
                    }
                }),
                labels: vec![],
            };

        let handle = self.driver.start(config).await?;
        self.handle = Some(handle);
        Ok(())
    }

    /// Connect to the FE gRPC server and initialize it.
    async fn connect_and_initialize(&mut self) -> Result<FunctionExecutorGrpcClient> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No process handle"))?;

        let addr = handle
            .daemon_addr
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No FE gRPC address"))?;

        info!(addr = %addr, "Connecting to function executor");

        // Retry connecting to the FE with process liveness checks.
        // If the FE process dies (e.g. port binding failure), we fail
        // immediately instead of waiting the full FE_READY_TIMEOUT.
        let mut client = {
            let deadline = tokio::time::Instant::now() + FE_READY_TIMEOUT;
            let poll_interval = Duration::from_millis(100);
            let mut last_err = None;

            loop {
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "Timeout connecting to function executor at {} after {:?}: {}",
                        addr,
                        FE_READY_TIMEOUT,
                        last_err
                            .as_ref()
                            .map(|e: &anyhow::Error| e.to_string())
                            .unwrap_or_default()
                    );
                }

                match FunctionExecutorGrpcClient::connect(addr).await {
                    Ok(c) => break c,
                    Err(e) => {
                        last_err = Some(e);
                    }
                }

                // Check if the FE process is still alive before retrying.
                // This catches crashes like port binding failures immediately.
                if let Some(h) = &self.handle &&
                    !self.driver.alive(h).await.unwrap_or(false)
                {
                    let exit_status = self.driver.get_exit_status(h).await.ok().flatten();
                    anyhow::bail!(
                        "Function executor process died before accepting connections \
                         (exit_status={:?}): {}",
                        exit_status,
                        last_err
                            .as_ref()
                            .map(|e: &anyhow::Error| e.to_string())
                            .unwrap_or_default()
                    );
                }

                tokio::time::sleep(poll_interval).await;
            }
        };

        // Get info to verify connectivity
        let info = client.get_info().await?;
        info!(
            version = ?info.version,
            sdk_version = ?info.sdk_version,
            sdk_language = ?info.sdk_language,
            "Connected to function executor"
        );

        // Download application code via cache
        let application_code = self.download_app_code().await?;

        // Initialize the FE with the function to run
        let init_request = proto_api::function_executor_pb::InitializeRequest {
            function: self.description.function.as_ref().map(|f| {
                proto_api::function_executor_pb::FunctionRef {
                    namespace: f.namespace.clone(),
                    application_name: f.application_name.clone(),
                    function_name: f.function_name.clone(),
                    application_version: f.application_version.clone(),
                }
            }),
            application_code,
        };

        let timeout_ms = self.description.initialization_timeout_ms.unwrap_or(0);
        let init_future = client.initialize(init_request);

        let init_response = if timeout_ms > 0 {
            tokio::time::timeout(Duration::from_millis(timeout_ms as u64), init_future)
                .await
                .map_err(|_| anyhow::anyhow!("FE initialization timed out after {}ms", timeout_ms))?
                .map_err(|e| {
                    error!(error = %e, "FE initialize RPC call failed");
                    e
                })?
        } else {
            init_future.await.map_err(|e| {
                error!(error = %e, "FE initialize RPC call failed");
                e
            })?
        };

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

        Ok(client)
    }

    /// Download application code using the code cache.
    async fn download_app_code(
        &self,
    ) -> Result<Option<proto_api::function_executor_pb::SerializedObject>> {
        let func_ref = self
            .description
            .function
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No function reference in description"))?;

        let namespace = func_ref.namespace.as_deref().unwrap_or("");
        let app_name = func_ref.application_name.as_deref().unwrap_or("");
        let app_version = func_ref.application_version.as_deref().unwrap_or("");

        let data_payload = self
            .description
            .application
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No application data payload in description"))?;

        let code_uri = data_payload.uri.as_deref().unwrap_or("");

        if code_uri.is_empty() {
            return Ok(None);
        }

        let expected_sha256 = data_payload.sha256_hash.as_deref();
        let code_bytes = self
            .code_cache
            .get_or_download(namespace, app_name, app_version, code_uri, expected_sha256)
            .await?;

        // Build the SerializedObjectManifest from the server's DataPayload metadata.
        // The FE requires encoding, encoding_version, size, metadata_size, and
        // sha256_hash.
        use proto_api::function_executor_pb::SerializedObjectManifest;

        // Map DataPayloadEncoding → SerializedObjectEncoding
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

    fn handle_add_allocation(&mut self, allocation: ServerAllocation) {
        let counters = crate::metrics::DataplaneCounters::new();
        let up_down = crate::metrics::DataplaneUpDownCounters::new();
        counters.allocations_fetched.add(1, &[]);

        // If we're terminated/terminating, don't accept new allocations
        if let Some(status) = self.state_tx.borrow().status &&
            status == FunctionExecutorStatus::Terminated as i32
        {
            warn!(
                allocation_id = ?allocation.allocation_id,
                "Rejecting allocation, FE is terminated"
            );

            let term_reason = self
                .state_tx
                .borrow()
                .termination_reason
                .and_then(|r| FunctionExecutorTerminationReason::try_from(r).ok());

            let failure_reason = match term_reason {
                Some(FunctionExecutorTerminationReason::StartupFailedFunctionTimeout) => {
                    proto_api::executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout
                }
                Some(FunctionExecutorTerminationReason::StartupFailedInternalError) => {
                    proto_api::executor_api_pb::AllocationFailureReason::StartupFailedInternalError
                }
                Some(FunctionExecutorTerminationReason::StartupFailedFunctionError) => {
                    proto_api::executor_api_pb::AllocationFailureReason::StartupFailedFunctionError
                }
                _ => proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated,
            };

            // We should probably report failure for this allocation immediately
            if let Some(alloc_id) = allocation.allocation_id {
                let result = proto_api::executor_api_pb::AllocationResult {
                    function: allocation.function.clone(),
                    allocation_id: Some(alloc_id),
                    function_call_id: allocation.function_call_id.clone(),
                    request_id: allocation.request_id.clone(),
                    outcome_code: Some(
                        proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into(),
                    ),
                    failure_reason: Some(failure_reason.into()),
                    return_value: None,
                    request_error: None,
                    execution_duration_ms: None,
                };
                let _ = self.result_tx.send(CompletedAllocation { result });
            }
            return;
        }

        let alloc_id = allocation.allocation_id.clone().unwrap_or_default();

        if self.allocations.contains_key(&alloc_id) {
            return; // Already tracking
        }

        let alloc_cancel = self.cancel_token.child_token();
        let info = AllocationInfo {
            allocation: allocation.clone(),
            state: AllocationState::Preparing,
            cancel_token: alloc_cancel,
            prepared: None,
            server_result: None,
            finalization_ctx: None,
        };

        self.allocations.insert(alloc_id.clone(), info);
        counters.allocation_preparations.add(1, &[]);
        up_down.allocations_getting_prepared.add(1, &[]);

        // Spawn prep task (does NOT occupy a concurrency slot)
        let event_tx = self.event_tx.clone();
        let blob_store = self.blob_store.clone();
        let alloc_id_clone = alloc_id.clone();
        tokio::spawn(async move {
            let prep_start = Instant::now();
            let result = allocation_prep::prepare_allocation(&allocation, &blob_store).await;
            let histograms = crate::metrics::DataplaneHistograms::new();
            let up_down = crate::metrics::DataplaneUpDownCounters::new();
            histograms
                .allocation_preparation_latency_seconds
                .record(prep_start.elapsed().as_secs_f64(), &[]);
            up_down.allocations_getting_prepared.add(-1, &[]);
            if result.is_err() {
                crate::metrics::DataplaneCounters::new()
                    .allocation_preparation_errors
                    .add(1, &[]);
            }
            let _ = event_tx.send(FEEvent::AllocationPreparationFinished {
                allocation_id: alloc_id_clone,
                result,
            });
        });
    }

    fn handle_schedule_execution(&mut self) {
        while self.running_set.len() < self.max_concurrency as usize {
            let Some(alloc_id) = self.runnable_queue.pop_front() else {
                break;
            };

            let Some(info) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };

            // If cancelled while in queue, route to finalization
            if info.cancel_token.is_cancelled() {
                info.server_result = Some(make_cancelled_result(&info.allocation));
                self.start_finalization(&alloc_id.clone());
                continue;
            }

            // If FE terminated while in queue, route to finalization
            let Some(client) = self.client.clone() else {
                warn!(
                    allocation_id = %alloc_id,
                    "Cannot schedule allocation, FE client is not available (terminated?)"
                );
                if let Some(info) = self.allocations.get_mut(&alloc_id) {
                    info.server_result = Some(make_fe_terminated_result(&info.allocation));
                }
                self.start_finalization(&alloc_id.clone());
                continue;
            };

            // Take prepared data
            let prepared = match info.prepared.take() {
                Some(p) => p,
                None => {
                    warn!(
                        allocation_id = %alloc_id,
                        "No prepared data for runnable allocation"
                    );
                    info.server_result = Some(make_internal_error_result(&info.allocation));
                    self.start_finalization(&alloc_id.clone());
                    continue;
                }
            };

            info.state = AllocationState::Running;
            self.running_set.insert(alloc_id.clone());

            let counters = crate::metrics::DataplaneCounters::new();
            let up_down = crate::metrics::DataplaneUpDownCounters::new();
            counters.allocation_runs.add(1, &[]);
            up_down.allocation_runs_in_progress.add(1, &[]);

            let allocation = info.allocation.clone();
            let cancel_token = info.cancel_token.clone();
            let event_tx = self.event_tx.clone();
            let server_channel = self.server_channel.clone();
            let blob_store = self.blob_store.clone();
            let alloc_id_clone = alloc_id.clone();
            let watcher_registry = self.watcher_registry.clone();
            let allocation_timeout = Duration::from_millis(
                self.description.allocation_timeout_ms.unwrap_or(300_000) as u64,
            );

            tokio::spawn(async move {
                let run_start = Instant::now();
                let result = super::allocation_runner::execute_allocation(
                    client,
                    allocation,
                    prepared,
                    server_channel,
                    blob_store,
                    allocation_timeout,
                    cancel_token,
                    watcher_registry,
                )
                .await;

                let histograms = crate::metrics::DataplaneHistograms::new();
                let up_down = crate::metrics::DataplaneUpDownCounters::new();
                histograms
                    .allocation_run_latency_seconds
                    .record(run_start.elapsed().as_secs_f64(), &[]);
                up_down.allocation_runs_in_progress.add(-1, &[]);

                let _ = event_tx.send(FEEvent::AllocationExecutionFinished {
                    allocation_id: alloc_id_clone,
                    result,
                });
            });
        }
    }

    async fn handle_event(&mut self, event: FEEvent) -> bool {
        match event {
            FEEvent::FunctionExecutorTerminated { fe_id, reason } => {
                self.handle_fe_terminated(fe_id, reason).await;
            }
            FEEvent::AllocationPreparationFinished {
                allocation_id,
                result,
            } => {
                self.handle_preparation_finished(&allocation_id, result);
            }
            FEEvent::ScheduleAllocationExecution => {
                self.handle_schedule_execution();
            }
            FEEvent::AllocationExecutionFinished {
                allocation_id,
                result,
            } => {
                self.handle_execution_finished(&allocation_id, result);
            }
            FEEvent::AllocationFinalizationFinished {
                allocation_id,
                is_success,
            } => {
                self.handle_finalization_finished(&allocation_id, is_success);
            }
        }
        false // don't exit
    }

    fn handle_preparation_finished(
        &mut self,
        allocation_id: &str,
        result: anyhow::Result<PreparedAllocation>,
    ) {
        let Some(info) = self.allocations.get_mut(allocation_id) else {
            return;
        };

        // If cancelled during prep, route to finalization
        if info.cancel_token.is_cancelled() {
            info.server_result = Some(make_cancelled_result(&info.allocation));
            // If prep succeeded, we have a request_error_blob_handle to clean up
            if let Ok(prepared) = result {
                info.finalization_ctx = Some(FinalizationContext {
                    request_error_blob_handle: prepared.request_error_blob_handle,
                    output_blob_handles: Vec::new(),
                    fe_result: None,
                });
            }
            self.start_finalization(allocation_id);
            return;
        }

        match result {
            Ok(prepared) => {
                // Initialize finalization context with the request error blob handle
                info.finalization_ctx = Some(FinalizationContext {
                    request_error_blob_handle: prepared.request_error_blob_handle.clone(),
                    output_blob_handles: Vec::new(),
                    fe_result: None,
                });
                info.prepared = Some(prepared);
                info.state = AllocationState::Runnable;
                self.runnable_queue.push_back(allocation_id.to_string());
                let _ = self.event_tx.send(FEEvent::ScheduleAllocationExecution);
            }
            Err(e) => {
                warn!(
                    allocation_id = %allocation_id,
                    error = %e,
                    "Allocation preparation failed"
                );
                info.server_result = Some(make_internal_error_result(&info.allocation));
                self.start_finalization(allocation_id);
            }
        }
    }

    fn handle_execution_finished(&mut self, allocation_id: &str, outcome: AllocationOutcome) {
        // Free concurrency slot immediately
        self.running_set.remove(allocation_id);

        // Pipeline next allocation
        let _ = self.event_tx.send(FEEvent::ScheduleAllocationExecution);

        let Some(info) = self.allocations.get_mut(allocation_id) else {
            return;
        };

        // Track whether the failure was due to a stream error (FE likely crashed)
        let mut trigger_fe_termination = false;

        // Extract data from outcome and update finalization context
        match outcome {
            AllocationOutcome::Completed {
                result,
                fe_result,
                output_blob_handles,
            } => {
                info.server_result = Some(result);
                if let Some(ref mut ctx) = info.finalization_ctx {
                    ctx.output_blob_handles = output_blob_handles;
                    ctx.fe_result = fe_result;
                }
            }
            AllocationOutcome::Cancelled {
                output_blob_handles,
            } => {
                let mut failure_reason =
                    proto_api::executor_api_pb::AllocationFailureReason::AllocationCancelled;

                // Check if we are terminated. If so, use the termination reason to determine
                // allocation failure reason.
                if let Some(status) = self.state_tx.borrow().status &&
                    status == FunctionExecutorStatus::Terminated as i32 &&
                    let Some(term_reason_i32) = self.state_tx.borrow().termination_reason &&
                    let Ok(term_reason) =
                        FunctionExecutorTerminationReason::try_from(term_reason_i32)
                {
                    failure_reason = match term_reason {
                        FunctionExecutorTerminationReason::Unhealthy => {
                            // Treat grey failures (process crash/unresponsive) as FunctionError
                            // to prevent service abuse by functions that crash the executor.
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionError
                        }
                        FunctionExecutorTerminationReason::InternalError => {
                            proto_api::executor_api_pb::AllocationFailureReason::InternalError
                        }
                        FunctionExecutorTerminationReason::FunctionTimeout => {
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout
                        }
                        FunctionExecutorTerminationReason::Oom => {
                            proto_api::executor_api_pb::AllocationFailureReason::Oom
                        }
                        _ => proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated,
                    };
                }

                let result = ServerAllocationResult {
                    function: info.allocation.function.clone(),
                    allocation_id: info.allocation.allocation_id.clone(),
                    function_call_id: info.allocation.function_call_id.clone(),
                    request_id: info.allocation.request_id.clone(),
                    outcome_code: Some(
                        proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into(),
                    ),
                    failure_reason: Some(failure_reason.into()),
                    return_value: None,
                    request_error: None,
                    execution_duration_ms: None,
                };
                info.server_result = Some(result);
                if let Some(ref mut ctx) = info.finalization_ctx {
                    ctx.output_blob_handles = output_blob_handles;
                }
            }
            AllocationOutcome::Failed {
                reason,
                error_message,
                output_blob_handles,
                likely_fe_crash,
            } => {
                // If the allocation failed due to a connection/stream error, the FE likely
                // crashed. Trigger immediate termination instead of waiting 15s
                // for the health checker.
                if likely_fe_crash {
                    warn!(
                        allocation_id = %allocation_id,
                        error = %error_message,
                        "Allocation failed due to likely FE crash, will terminate FE"
                    );
                    trigger_fe_termination = true;
                }

                let result = ServerAllocationResult {
                    function: info.allocation.function.clone(),
                    allocation_id: info.allocation.allocation_id.clone(),
                    function_call_id: info.allocation.function_call_id.clone(),
                    request_id: info.allocation.request_id.clone(),
                    outcome_code: Some(
                        proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into(),
                    ),
                    failure_reason: Some(reason.into()),
                    return_value: None,
                    request_error: None,
                    execution_duration_ms: None,
                };
                info.server_result = Some(result);
                if let Some(ref mut ctx) = info.finalization_ctx {
                    ctx.output_blob_handles = output_blob_handles;
                }
            }
        }

        self.start_finalization(allocation_id);

        if trigger_fe_termination {
            let fe_id = self.description.id.clone().unwrap_or_default();
            let _ = self.event_tx.send(FEEvent::FunctionExecutorTerminated {
                fe_id,
                reason: FunctionExecutorTerminationReason::Unhealthy,
            });
        }
    }

    fn start_finalization(&mut self, allocation_id: &str) {
        let Some(info) = self.allocations.get_mut(allocation_id) else {
            return;
        };

        info.state = AllocationState::Finalizing;

        let counters = crate::metrics::DataplaneCounters::new();
        let up_down = crate::metrics::DataplaneUpDownCounters::new();
        counters.allocation_finalizations.add(1, &[]);
        up_down.allocations_finalizing.add(1, &[]);

        // Take finalization context (may be None if prep never completed)
        let ctx = info.finalization_ctx.take().unwrap_or(FinalizationContext {
            request_error_blob_handle: None,
            output_blob_handles: Vec::new(),
            fe_result: None,
        });

        let event_tx = self.event_tx.clone();
        let blob_store = self.blob_store.clone();
        let alloc_id = allocation_id.to_string();

        tokio::spawn(async move {
            let finalization_start = Instant::now();
            let is_success = allocation_finalize::finalize_allocation(
                &alloc_id,
                ctx.fe_result.as_ref(),
                ctx.request_error_blob_handle.as_ref(),
                &ctx.output_blob_handles,
                &blob_store,
            )
            .await
            .is_ok();

            let histograms = crate::metrics::DataplaneHistograms::new();
            let up_down = crate::metrics::DataplaneUpDownCounters::new();
            histograms
                .allocation_finalization_latency_seconds
                .record(finalization_start.elapsed().as_secs_f64(), &[]);
            up_down.allocations_finalizing.add(-1, &[]);
            if !is_success {
                crate::metrics::DataplaneCounters::new()
                    .allocation_finalization_errors
                    .add(1, &[]);
            }

            let _ = event_tx.send(FEEvent::AllocationFinalizationFinished {
                allocation_id: alloc_id,
                is_success,
            });
        });
    }

    fn handle_finalization_finished(&mut self, allocation_id: &str, is_success: bool) {
        let Some(info) = self.allocations.get_mut(allocation_id) else {
            return;
        };

        if !is_success {
            // Override result with internal error, but preserve execution_duration_ms
            let existing_duration = info
                .server_result
                .as_ref()
                .and_then(|r| r.execution_duration_ms);
            let mut err_result = make_internal_error_result(&info.allocation);
            err_result.execution_duration_ms = existing_duration;
            info.server_result = Some(err_result);
        } else if info.server_result.is_none() {
            // Should not happen, but if server_result is missing, set internal error
            warn!(allocation_id = %allocation_id, "Server result missing after finalization");
            info.server_result = Some(make_internal_error_result(&info.allocation));
        }

        info.state = AllocationState::Done;

        if let Some(result) = info.server_result.take() {
            // Record allocation outcome metrics
            record_allocation_metrics(&result);
            let _ = self.result_tx.send(CompletedAllocation { result });
        }
    }

    async fn handle_fe_terminated(
        &mut self,
        fe_id: String,
        reason: FunctionExecutorTerminationReason,
    ) {
        if Some(&fe_id) != self.description.id.as_ref() {
            warn!(
                received_fe_id = %fe_id,
                expected_fe_id = ?self.description.id,
                "Ignoring FE terminated event for mismatched fe_id"
            );
            return;
        }
        warn!(reason = ?reason, "Function executor terminated");

        // Cancel the health checker immediately
        if let Some(token) = &self.fe_process_cancel_token {
            token.cancel();
        }

        // Cancel all allocation tokens
        for info in self.allocations.values() {
            info.cancel_token.cancel();
        }

        // For allocations that are already Done or Finalizing, let them finish
        // normally. For allocations in Preparing, their prep task will fire the
        // event and handle_preparation_finished will see the cancellation.
        // For allocations in Runnable, drain them through finalization now.
        let runnable_ids: Vec<String> = self.runnable_queue.drain(..).collect();
        for alloc_id in &runnable_ids {
            if let Some(info) = self.allocations.get_mut(alloc_id) &&
                info.state == AllocationState::Runnable
            {
                info.server_result = Some(make_fe_terminated_result(&info.allocation));
                // start_finalization needs &mut self, so we do it below
            }
        }
        for alloc_id in runnable_ids {
            self.start_finalization(&alloc_id);
        }

        // For Running allocations, the cancel token was cancelled above.
        // Their execution tasks will return Cancelled outcomes, which will
        // route through handle_execution_finished → start_finalization.

        // Kill process
        if let Some(handle) = &self.handle {
            let _ = self.driver.kill(handle).await;
        }
        self.transition_to_terminated(reason);
    }

    async fn shutdown(&mut self) {
        info!("Shutting down function executor controller");
        let counters = crate::metrics::DataplaneCounters::new();
        let histograms = crate::metrics::DataplaneHistograms::new();
        let up_down = crate::metrics::DataplaneUpDownCounters::new();
        counters.function_executor_destroys.add(1, &[]);
        let destroy_start = Instant::now();

        // Cancel all allocations
        for info in self.allocations.values() {
            info.cancel_token.cancel();
        }

        // Cancel the health checker immediately
        if let Some(token) = &self.fe_process_cancel_token {
            token.cancel();
        }

        // Kill the process
        if let Some(handle) = &self.handle {
            let _ = self.driver.kill(handle).await;
        }
        histograms
            .function_executor_destroy_latency_seconds
            .record(destroy_start.elapsed().as_secs_f64(), &[]);
        up_down.function_executors_count.add(-1, &[]);
        self.transition_to_terminated(FunctionExecutorTerminationReason::FunctionCancelled);
    }

    fn transition_to_terminated(&mut self, reason: FunctionExecutorTerminationReason) {
        // Stop health checker if it's still running
        if let Some(token) = &self.fe_process_cancel_token {
            token.cancel();
        }
        self.client = None; // Client is invalid now
        self.update_state(FunctionExecutorStatus::Terminated, Some(reason));
    }

    fn update_state(
        &self,
        status: FunctionExecutorStatus,
        termination_reason: Option<FunctionExecutorTerminationReason>,
    ) {
        let state = FunctionExecutorState {
            description: Some(self.description.clone()),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
            allocation_ids_caused_termination: vec![],
        };
        let _ = self.state_tx.send(state);
    }
}

/// Build a cancelled AllocationResult for the given allocation.
fn make_cancelled_result(allocation: &ServerAllocation) -> ServerAllocationResult {
    ServerAllocationResult {
        function: allocation.function.clone(),
        allocation_id: allocation.allocation_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        request_id: allocation.request_id.clone(),
        outcome_code: Some(proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into()),
        failure_reason: Some(
            proto_api::executor_api_pb::AllocationFailureReason::AllocationCancelled.into(),
        ),
        return_value: None,
        request_error: None,
        execution_duration_ms: None,
    }
}

/// Build an internal error AllocationResult for the given allocation.
fn make_internal_error_result(allocation: &ServerAllocation) -> ServerAllocationResult {
    ServerAllocationResult {
        function: allocation.function.clone(),
        allocation_id: allocation.allocation_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        request_id: allocation.request_id.clone(),
        outcome_code: Some(proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into()),
        failure_reason: Some(
            proto_api::executor_api_pb::AllocationFailureReason::InternalError.into(),
        ),
        return_value: None,
        request_error: None,
        execution_duration_ms: None,
    }
}

/// Record allocation outcome metrics using the global OpenTelemetry meter.
fn record_allocation_metrics(result: &ServerAllocationResult) {
    use crate::metrics::DataplaneCounters;

    let counters = DataplaneCounters::new();
    let outcome_code = result.outcome_code.unwrap_or(0);
    let outcome =
        if outcome_code == proto_api::executor_api_pb::AllocationOutcomeCode::Success as i32 {
            "success"
        } else {
            "failure"
        };

    let failure_reason = if outcome == "failure" {
        result.failure_reason.and_then(|r| {
            proto_api::executor_api_pb::AllocationFailureReason::try_from(r)
                .ok()
                .map(|reason| format!("{:?}", reason))
        })
    } else {
        None
    };

    counters.record_allocation_completed(
        outcome,
        failure_reason.as_deref(),
        result.execution_duration_ms,
    );
}

/// Build an FE-terminated AllocationResult for the given allocation.
fn make_fe_terminated_result(allocation: &ServerAllocation) -> ServerAllocationResult {
    ServerAllocationResult {
        function: allocation.function.clone(),
        allocation_id: allocation.allocation_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        request_id: allocation.request_id.clone(),
        outcome_code: Some(proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into()),
        failure_reason: Some(
            proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated.into(),
        ),
        return_value: None,
        request_error: None,
        execution_duration_ms: None,
    }
}
