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
    AllocationFailureReason,
    AllocationOutcomeCode,
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
    events::{AllocationOutcome, FECommand, FEEvent, FinalizationContext, PreparedAllocation},
    fe_client::FunctionExecutorGrpcClient,
    health_checker,
    proto_convert,
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

/// Typed error for FE initialization timeout (replaces string-based detection).
#[derive(Debug)]
struct InitTimedOut(u64);

impl std::fmt::Display for InitTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FE initialization timed out after {}ms", self.0)
    }
}

impl std::error::Error for InitTimedOut {}

/// Shared configuration for spawning function executor controllers.
#[derive(Clone)]
pub struct FESpawnConfig {
    pub driver: Arc<dyn ProcessDriver>,
    pub image_resolver: Arc<dyn ImageResolver>,
    pub result_tx: mpsc::UnboundedSender<ServerAllocationResult>,
    pub server_channel: Channel,
    pub blob_store: Arc<BlobStore>,
    pub code_cache: Arc<CodeCache>,
    pub executor_id: String,
    pub fe_binary_path: String,
    pub metrics: Arc<crate::metrics::DataplaneMetrics>,
}

/// Handle returned to the StateReconciler for communicating with a controller.
pub struct FEControllerHandle {
    /// Send commands (new allocations, shutdown).
    pub command_tx: mpsc::UnboundedSender<FECommand>,
    /// Read current FE state for heartbeat reporting.
    pub state_rx: watch::Receiver<FunctionExecutorState>,
}

/// State of an allocation tracked by the controller.
///
/// Phase transitions follow this graph:
///
/// ```text
///   Preparing ──► Runnable ──► Running ──► Finalizing ──► Done
///       │             │                        ▲
///       └─────────────┴── (on failure/cancel) ─┘
/// ```
///
/// Use the `transition_*` and `take_*` methods to move between phases.
/// These encapsulate the `mem::replace` pattern and keep the invariants
/// in one place.
struct AllocationInfo {
    allocation: ServerAllocation,
    cancel_token: CancellationToken,
    phase: AllocationPhase,
}

impl AllocationInfo {
    /// Transition from Preparing → Runnable with prepared data.
    ///
    /// A `FinalizationContext` is created automatically from the prepared
    /// allocation's `request_error_blob_handle`.
    fn transition_to_runnable(&mut self, prepared: PreparedAllocation) {
        let finalization_ctx = FinalizationContext {
            request_error_blob_handle: prepared.request_error_blob_handle.clone(),
            output_blob_handles: Vec::new(),
            fe_result: None,
        };
        self.phase = AllocationPhase::Runnable {
            prepared,
            finalization_ctx,
        };
    }

    /// Transition from Runnable → Running, returning the `PreparedAllocation`.
    ///
    /// Returns `None` if not in the Runnable phase (phase is left unchanged).
    fn take_prepared_and_run(&mut self) -> Option<PreparedAllocation> {
        match std::mem::replace(&mut self.phase, AllocationPhase::Done) {
            AllocationPhase::Runnable {
                prepared,
                finalization_ctx,
            } => {
                self.phase = AllocationPhase::Running { finalization_ctx };
                Some(prepared)
            }
            other => {
                self.phase = other;
                None
            }
        }
    }

    /// Extract the `FinalizationContext` from Runnable or Running phases.
    ///
    /// For other phases, returns a default context (phase unchanged). This
    /// is intentional: error/cancellation paths may need a context even when
    /// the phase is unexpected.
    fn take_finalization_ctx(&mut self) -> FinalizationContext {
        match std::mem::replace(&mut self.phase, AllocationPhase::Done) {
            AllocationPhase::Runnable {
                finalization_ctx, ..
            } |
            AllocationPhase::Running { finalization_ctx } => finalization_ctx,
            other => {
                self.phase = other;
                FinalizationContext::default()
            }
        }
    }

    /// Set the phase to Finalizing with the given server result.
    fn transition_to_finalizing(&mut self, server_result: ServerAllocationResult) {
        self.phase = AllocationPhase::Finalizing { server_result };
    }

    /// Extract the server result from the Finalizing phase.
    ///
    /// Returns `None` if not in the Finalizing phase (phase unchanged).
    fn take_server_result(&mut self) -> Option<ServerAllocationResult> {
        match std::mem::replace(&mut self.phase, AllocationPhase::Done) {
            AllocationPhase::Finalizing { server_result } => Some(server_result),
            other => {
                self.phase = other;
                None
            }
        }
    }
}

/// Allocation lifecycle phase with data carried per variant.
///
/// Data moves forward through phases and is consumed at transitions,
/// eliminating the `Option` fields of the previous design.
enum AllocationPhase {
    /// Waiting for input preparation (presigning blobs).
    Preparing,
    /// Inputs ready, queued for a concurrency slot.
    Runnable {
        prepared: PreparedAllocation,
        finalization_ctx: FinalizationContext,
    },
    /// Executing on the FE subprocess (occupies a concurrency slot).
    Running {
        finalization_ctx: FinalizationContext,
    },
    /// Post-execution cleanup (completing multipart uploads, etc.).
    Finalizing {
        server_result: ServerAllocationResult,
    },
    /// Terminal state — result has been sent.
    Done,
}

/// Tracks allocations through their lifecycle phases with coupled data
/// structures.
///
/// Centralizes the `allocations` map, `runnable_queue`, and `running_set`
/// to prevent them from drifting out of sync.
struct AllocationTracker {
    allocations: HashMap<String, AllocationInfo>,
    runnable_queue: VecDeque<String>,
    running_set: HashSet<String>,
}

impl AllocationTracker {
    fn new() -> Self {
        Self {
            allocations: HashMap::new(),
            runnable_queue: VecDeque::new(),
            running_set: HashSet::new(),
        }
    }

    fn contains(&self, id: &str) -> bool {
        self.allocations.contains_key(id)
    }

    fn insert(&mut self, id: String, info: AllocationInfo) {
        self.allocations.insert(id, info);
    }

    fn get_mut(&mut self, id: &str) -> Option<&mut AllocationInfo> {
        self.allocations.get_mut(id)
    }

    fn cancel_all_tokens(&self) {
        for info in self.allocations.values() {
            info.cancel_token.cancel();
        }
    }

    fn enqueue_runnable(&mut self, id: String) {
        self.runnable_queue.push_back(id);
    }

    fn dequeue_runnable(&mut self) -> Option<String> {
        self.runnable_queue.pop_front()
    }

    fn drain_runnable(&mut self) -> Vec<String> {
        self.runnable_queue.drain(..).collect()
    }

    fn mark_running(&mut self, id: String) {
        self.running_set.insert(id);
    }

    fn unmark_running(&mut self, id: &str) {
        self.running_set.remove(id);
    }

    fn running_count(&self) -> usize {
        self.running_set.len()
    }
}

/// The main controller that manages one function executor process.
pub struct FunctionExecutorController {
    description: FunctionExecutorDescription,
    config: FESpawnConfig,
    // Process lifecycle
    handle: Option<ProcessHandle>,
    // gRPC client to the FE subprocess
    client: Option<FunctionExecutorGrpcClient>,
    // Inbound events from background tasks
    event_tx: mpsc::UnboundedSender<FEEvent>,
    event_rx: mpsc::UnboundedReceiver<FEEvent>,
    // Inbound commands from StateReconciler
    command_rx: mpsc::UnboundedReceiver<FECommand>,
    // Outbound: current FE state for heartbeat reporting
    state_tx: watch::Sender<FunctionExecutorState>,
    // Allocation tracking
    tracker: AllocationTracker,
    max_concurrency: u32,
    // Cancellation hierarchy
    cancel_token: CancellationToken,
    // Watcher registry for function call result routing
    watcher_registry: WatcherRegistry,
    // Token to cancel the health checker when the process is killed
    fe_process_cancel_token: Option<CancellationToken>,
}

impl FunctionExecutorController {
    /// Spawn a new controller as a tokio task. Returns a handle for
    /// communication.
    pub fn spawn(
        description: FunctionExecutorDescription,
        config: FESpawnConfig,
        cancel_token: CancellationToken,
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

        let mut controller = Self {
            description,
            config,
            handle: None,
            client: None,
            event_tx,
            event_rx,
            command_rx,
            state_tx,
            tracker: AllocationTracker::new(),
            max_concurrency,
            cancel_token,
            watcher_registry,
            fe_process_cancel_token: None,
        };

        tokio::spawn(
            async move {
                controller.run().await;
            }
            .instrument(tracing::info_span!("fe_controller", fe_id = %fe_id, namespace = %namespace, app = %app, fn_name = %fn_name)),
        );

        FEControllerHandle {
            command_tx,
            state_rx,
        }
    }

    /// Main control loop.
    async fn run(&mut self) {
        let metrics = self.config.metrics.clone();
        let counters = &metrics.counters;
        let histograms = &metrics.histograms;
        let up_down = &metrics.up_down_counters;

        let create_start = Instant::now();
        counters.function_executor_creates.add(1, &[]);
        up_down.function_executors_count.add(1, &[]);

        // Phase 1+2: Start process, connect, and initialize
        let client = match self.start_and_initialize().await {
            Ok(client) => client,
            Err(reason) => {
                counters.function_executor_create_errors.add(1, &[]);
                histograms
                    .function_executor_create_latency_seconds
                    .record(create_start.elapsed().as_secs_f64(), &[]);
                up_down.function_executors_count.add(-1, &[]);
                self.transition_to_terminated(reason);

                // Drain command channel to handle any allocations that were sent
                // while we were initializing
                self.command_rx.close();
                while let Some(cmd) = self.command_rx.recv().await {
                    if let FECommand::AddAllocation(alloc) = cmd {
                        self.handle_add_allocation(alloc);
                    }
                }
                return;
            }
        };

        histograms
            .function_executor_create_latency_seconds
            .record(create_start.elapsed().as_secs_f64(), &[]);
        self.client = Some(client.clone());
        self.update_state(FunctionExecutorStatus::Running, None);

        // Spawn health checker
        let process_token = CancellationToken::new();
        self.fe_process_cancel_token = Some(process_token.clone());
        let fe_id = self.description.id.clone().unwrap_or_default();
        let event_tx = self.event_tx.clone();
        tokio::spawn(health_checker::run_health_checker(
            client,
            self.config.driver.clone(),
            self.handle.clone().expect("handle set after start_process"),
            event_tx,
            process_token,
            fe_id,
        ));

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
                    self.handle_event(event).await;
                }
            }
        }
    }

    /// Start the FE process, connect, and initialize it.
    ///
    /// Returns `Ok(client)` on success, or `Err(termination_reason)` on
    /// failure (the process is killed on connect/init failure).
    async fn start_and_initialize(
        &mut self,
    ) -> std::result::Result<FunctionExecutorGrpcClient, FunctionExecutorTerminationReason> {
        let metrics = self.config.metrics.clone();

        // Start the FE process
        info!(fe_id = ?self.description.id, "Starting function executor");
        let start_process_time = Instant::now();
        if let Err(e) = self.start_fe_process().await {
            error!(error = %e, "Failed to start function executor process");
            metrics
                .counters
                .function_executor_create_server_errors
                .add(1, &[]);
            metrics
                .histograms
                .function_executor_create_server_latency_seconds
                .record(start_process_time.elapsed().as_secs_f64(), &[]);
            return Err(FunctionExecutorTerminationReason::StartupFailedInternalError);
        }
        metrics
            .histograms
            .function_executor_create_server_latency_seconds
            .record(start_process_time.elapsed().as_secs_f64(), &[]);

        // Connect to FE and initialize
        match self.connect_and_initialize().await {
            Ok(client) => Ok(client),
            Err(e) => {
                info!(error = ?e, "Failed to connect/initialize function executor");
                // Kill the process since we can't use it
                if let Some(handle) = &self.handle {
                    let _ = self.config.driver.kill(handle).await;
                }

                let reason = if e.is::<InitTimedOut>() {
                    FunctionExecutorTerminationReason::StartupFailedFunctionTimeout
                } else {
                    FunctionExecutorTerminationReason::StartupFailedInternalError
                };
                Err(reason)
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
            .config
            .image_resolver
            .function_image(namespace, app, function, version)
            .ok();

        // Build environment variables
        let env = vec![
            (
                "INDEXIFY_EXECUTOR_ID".to_string(),
                self.config.executor_id.clone(),
            ),
            ("INDEXIFY_FE_ID".to_string(), fe_id.clone()),
        ];

        let config =
            ProcessConfig {
                id: fe_id.clone(),
                process_type: ProcessType::Function,
                image,
                command: self.config.fe_binary_path.clone(),
                args: vec![
                    format!("--executor-id={}", self.config.executor_id),
                    format!("--function-executor-id={}", fe_id),
                ],
                env,
                working_dir: None,
                resources: self.description.resources.as_ref().map(|r| {
                    crate::driver::ResourceLimits {
                        cpu_millicores: r.cpu_ms_per_sec.map(|v| v as u64),
                        memory_bytes: r.memory_bytes,
                        gpu_count: None,
                    }
                }),
                labels: vec![],
            };

        let handle = self.config.driver.start(config).await?;
        self.handle = Some(handle);
        Ok(())
    }

    /// Connect to the FE gRPC server and initialize it.
    async fn connect_and_initialize(&mut self) -> Result<FunctionExecutorGrpcClient> {
        let mut client = self.connect_to_fe().await?;
        self.initialize_fe(&mut client).await?;
        Ok(client)
    }

    /// Connect to the FE gRPC server with retry and verify connectivity.
    async fn connect_to_fe(&self) -> Result<FunctionExecutorGrpcClient> {
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
        let driver = self.config.driver.clone();
        let process_handle = self.handle.clone();
        let addr_owned = addr.to_string();
        let mut client = crate::retry::retry_until_deadline(
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
        .await?;

        // Verify connectivity
        let info = client.get_info().await?;
        info!(
            version = ?info.version,
            sdk_version = ?info.sdk_version,
            sdk_language = ?info.sdk_language,
            "Connected to function executor"
        );

        Ok(client)
    }

    /// Download application code and send initialization RPC.
    async fn initialize_fe(&self, client: &mut FunctionExecutorGrpcClient) -> Result<()> {
        let application_code = self.download_app_code().await?;

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
                .map_err(|_| InitTimedOut(timeout_ms as u64))?
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

        Ok(())
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
            .config
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
        self.config.metrics.counters.allocations_fetched.add(1, &[]);

        // If we're terminated/terminating, don't accept new allocations
        if let Some(status) = self.state_tx.borrow().status &&
            status == FunctionExecutorStatus::Terminated as i32
        {
            warn!(
                allocation_id = ?allocation.allocation_id,
                "Rejecting allocation, FE is terminated"
            );

            let failure_reason = self
                .state_tx
                .borrow()
                .termination_reason
                .and_then(|r| FunctionExecutorTerminationReason::try_from(r).ok())
                .map(proto_convert::termination_to_failure_reason)
                .unwrap_or(AllocationFailureReason::FunctionExecutorTerminated);

            // Report failure for this allocation immediately
            if allocation.allocation_id.is_some() {
                let result = proto_convert::make_failure_result(&allocation, failure_reason);
                let _ = self.config.result_tx.send(result);
            }
            return;
        }

        let alloc_id = allocation.allocation_id.clone().unwrap_or_default();

        if self.tracker.contains(&alloc_id) {
            return; // Already tracking
        }

        let alloc_cancel = self.cancel_token.child_token();
        let info = AllocationInfo {
            allocation: allocation.clone(),
            cancel_token: alloc_cancel,
            phase: AllocationPhase::Preparing,
        };

        self.tracker.insert(alloc_id.clone(), info);
        self.config
            .metrics
            .counters
            .allocation_preparations
            .add(1, &[]);
        self.config
            .metrics
            .up_down_counters
            .allocations_getting_prepared
            .add(1, &[]);

        // Spawn prep task (does NOT occupy a concurrency slot)
        let event_tx = self.event_tx.clone();
        let blob_store = self.config.blob_store.clone();
        let alloc_id_clone = alloc_id.clone();
        let request_id = allocation.request_id.clone().unwrap_or_default();
        let metrics = self.config.metrics.clone();
        tokio::spawn(
            async move {
                let result = timed_phase(
                    &metrics.histograms.allocation_preparation_latency_seconds,
                    &metrics.up_down_counters.allocations_getting_prepared,
                    Some(&metrics.counters.allocation_preparation_errors),
                    allocation_prep::prepare_allocation(&allocation, &blob_store),
                    |r: &Result<_>| r.is_err(),
                )
                .await;
                let _ = event_tx.send(FEEvent::AllocationPreparationFinished {
                    allocation_id: alloc_id_clone,
                    result,
                });
            }
            .instrument(tracing::info_span!("allocation_prep", allocation_id = %alloc_id, request_id = %request_id)),
        );
    }

    /// Fail an allocation by routing it through finalization with the given
    /// failure reason.
    fn fail_allocation(&mut self, allocation_id: &str, reason: AllocationFailureReason) {
        let Some(info) = self.tracker.get_mut(allocation_id) else {
            return;
        };
        let result = proto_convert::make_failure_result(&info.allocation, reason);
        let ctx = info.take_finalization_ctx();
        self.start_finalization(allocation_id, result, ctx);
    }

    fn handle_schedule_execution(&mut self) {
        while self.tracker.running_count() < self.max_concurrency as usize {
            let Some(alloc_id) = self.tracker.dequeue_runnable() else {
                break;
            };

            let Some(info) = self.tracker.get_mut(&alloc_id) else {
                continue;
            };

            // If cancelled while in queue, route to finalization
            if info.cancel_token.is_cancelled() {
                self.fail_allocation(&alloc_id, AllocationFailureReason::AllocationCancelled);
                continue;
            }

            // If FE terminated while in queue, route to finalization
            let Some(client) = self.client.clone() else {
                warn!(
                    allocation_id = %alloc_id,
                    "Cannot schedule allocation, FE client is not available (terminated?)"
                );
                self.fail_allocation(
                    &alloc_id,
                    AllocationFailureReason::FunctionExecutorTerminated,
                );
                continue;
            };

            // Transition Runnable → Running, taking the prepared data
            let Some(prepared) = info.take_prepared_and_run() else {
                warn!(
                    allocation_id = %alloc_id,
                    "Expected Runnable phase for scheduled allocation"
                );
                self.fail_allocation(&alloc_id, AllocationFailureReason::InternalError);
                continue;
            };
            // Clone data from info before releasing the tracker borrow
            let allocation = info.allocation.clone();
            let cancel_token = info.cancel_token.clone();

            self.tracker.mark_running(alloc_id.clone());

            self.config.metrics.counters.allocation_runs.add(1, &[]);
            self.config
                .metrics
                .up_down_counters
                .allocation_runs_in_progress
                .add(1, &[]);
            let event_tx = self.event_tx.clone();
            let alloc_id_clone = alloc_id.clone();
            let metrics = self.config.metrics.clone();
            let allocation_timeout = Duration::from_millis(
                self.description.allocation_timeout_ms.unwrap_or(300_000) as u64,
            );

            let ctx = super::allocation_runner::AllocationContext {
                server_channel: self.config.server_channel.clone(),
                blob_store: self.config.blob_store.clone(),
                watcher_registry: self.watcher_registry.clone(),
                metrics: metrics.clone(),
            };

            let request_id = allocation.request_id.clone().unwrap_or_default();
            tokio::spawn(
                async move {
                    let result = timed_phase(
                        &metrics.histograms.allocation_run_latency_seconds,
                        &metrics.up_down_counters.allocation_runs_in_progress,
                        None,
                        super::allocation_runner::execute_allocation(
                            client,
                            allocation,
                            prepared,
                            ctx,
                            allocation_timeout,
                            cancel_token,
                        ),
                        |_: &AllocationOutcome| false,
                    )
                    .await;

                    let _ = event_tx.send(FEEvent::AllocationExecutionFinished {
                        allocation_id: alloc_id_clone,
                        result,
                    });
                }
                .instrument(tracing::info_span!("allocation_exec", allocation_id = %alloc_id, request_id = %request_id)),
            );
        }
    }

    async fn handle_event(&mut self, event: FEEvent) {
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
    }

    fn handle_preparation_finished(
        &mut self,
        allocation_id: &str,
        result: anyhow::Result<PreparedAllocation>,
    ) {
        let Some(info) = self.tracker.get_mut(allocation_id) else {
            return;
        };

        // If cancelled during prep, route to finalization
        if info.cancel_token.is_cancelled() {
            let server_result = proto_convert::make_failure_result(
                &info.allocation,
                AllocationFailureReason::AllocationCancelled,
            );
            let ctx = if let Ok(prepared) = result {
                FinalizationContext {
                    request_error_blob_handle: prepared.request_error_blob_handle,
                    output_blob_handles: Vec::new(),
                    fe_result: None,
                }
            } else {
                FinalizationContext::default()
            };
            self.start_finalization(allocation_id, server_result, ctx);
            return;
        }

        match result {
            Ok(prepared) => {
                info.transition_to_runnable(prepared);
                self.tracker.enqueue_runnable(allocation_id.to_string());
                let _ = self.event_tx.send(FEEvent::ScheduleAllocationExecution);
            }
            Err(e) => {
                warn!(
                    allocation_id = %allocation_id,
                    error = %e,
                    "Allocation preparation failed"
                );
                let server_result = proto_convert::make_failure_result(
                    &info.allocation,
                    AllocationFailureReason::InternalError,
                );
                self.start_finalization(
                    allocation_id,
                    server_result,
                    FinalizationContext::default(),
                );
            }
        }
    }

    fn handle_execution_finished(&mut self, allocation_id: &str, outcome: AllocationOutcome) {
        // Free concurrency slot immediately
        self.tracker.unmark_running(allocation_id);

        // Pipeline next allocation
        let _ = self.event_tx.send(FEEvent::ScheduleAllocationExecution);

        let Some(info) = self.tracker.get_mut(allocation_id) else {
            return;
        };

        // Take finalization_ctx from Running phase
        let mut finalization_ctx = info.take_finalization_ctx();

        // Track whether the failure was due to a stream error (FE likely crashed)
        let mut trigger_fe_termination = false;

        // Extract data from outcome, build server_result, and update finalization
        // context
        let server_result = match outcome {
            AllocationOutcome::Completed {
                result,
                fe_result,
                output_blob_handles,
            } => {
                finalization_ctx.output_blob_handles = output_blob_handles;
                finalization_ctx.fe_result = fe_result;
                result
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
                    failure_reason = proto_convert::termination_to_failure_reason(term_reason);
                }

                finalization_ctx.output_blob_handles = output_blob_handles;
                proto_convert::make_failure_result(&info.allocation, failure_reason)
            }
            AllocationOutcome::Failed {
                reason,
                error_message,
                output_blob_handles,
                likely_fe_crash,
            } => {
                if likely_fe_crash {
                    warn!(
                        allocation_id = %allocation_id,
                        error = %error_message,
                        "Allocation failed due to likely FE crash, will terminate FE"
                    );
                    trigger_fe_termination = true;
                }

                finalization_ctx.output_blob_handles = output_blob_handles;
                proto_convert::make_failure_result(&info.allocation, reason)
            }
        };

        self.start_finalization(allocation_id, server_result, finalization_ctx);

        if trigger_fe_termination {
            let fe_id = self.description.id.clone().unwrap_or_default();
            let _ = self.event_tx.send(FEEvent::FunctionExecutorTerminated {
                fe_id,
                reason: FunctionExecutorTerminationReason::Unhealthy,
            });
        }
    }

    fn start_finalization(
        &mut self,
        allocation_id: &str,
        server_result: ServerAllocationResult,
        ctx: FinalizationContext,
    ) {
        let Some(info) = self.tracker.get_mut(allocation_id) else {
            return;
        };

        self.config
            .metrics
            .counters
            .allocation_finalizations
            .add(1, &[]);
        self.config
            .metrics
            .up_down_counters
            .allocations_finalizing
            .add(1, &[]);

        info.transition_to_finalizing(server_result);

        let event_tx = self.event_tx.clone();
        let blob_store = self.config.blob_store.clone();
        let alloc_id = allocation_id.to_string();
        let request_id = info.allocation.request_id.clone().unwrap_or_default();
        let metrics = self.config.metrics.clone();

        tokio::spawn(
            async move {
                let is_success = timed_phase(
                    &metrics.histograms.allocation_finalization_latency_seconds,
                    &metrics.up_down_counters.allocations_finalizing,
                    Some(&metrics.counters.allocation_finalization_errors),
                    async {
                        allocation_finalize::finalize_allocation(
                            &alloc_id,
                            ctx.fe_result.as_ref(),
                            ctx.request_error_blob_handle.as_ref(),
                            &ctx.output_blob_handles,
                            &blob_store,
                        )
                        .await
                        .is_ok()
                    },
                    |success: &bool| !success,
                )
                .await;

                let _ = event_tx.send(FEEvent::AllocationFinalizationFinished {
                    allocation_id: alloc_id,
                    is_success,
                });
            }
            .instrument(tracing::info_span!("allocation_finalize", allocation_id = %allocation_id, request_id = %request_id)),
        );
    }

    fn handle_finalization_finished(&mut self, allocation_id: &str, is_success: bool) {
        let Some(info) = self.tracker.get_mut(allocation_id) else {
            return;
        };

        // Extract server_result from Finalizing phase
        let Some(server_result) = info.take_server_result() else {
            warn!(allocation_id = %allocation_id, "Expected Finalizing phase");
            return;
        };

        let result = if !is_success {
            // Override result with internal error, but preserve execution_duration_ms
            let mut err_result = proto_convert::make_failure_result(
                &info.allocation,
                AllocationFailureReason::InternalError,
            );
            err_result.execution_duration_ms = server_result.execution_duration_ms;
            err_result
        } else {
            server_result
        };

        // Record allocation outcome metrics
        record_allocation_metrics(&result, &self.config.metrics.counters);
        let _ = self.config.result_tx.send(result);
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
        self.terminate(reason, true).await;
    }

    async fn shutdown(&mut self) {
        info!("Shutting down function executor controller");
        self.config
            .metrics
            .counters
            .function_executor_destroys
            .add(1, &[]);
        let destroy_start = Instant::now();

        self.terminate(FunctionExecutorTerminationReason::FunctionCancelled, false)
            .await;

        self.config
            .metrics
            .histograms
            .function_executor_destroy_latency_seconds
            .record(destroy_start.elapsed().as_secs_f64(), &[]);
        self.config
            .metrics
            .up_down_counters
            .function_executors_count
            .add(-1, &[]);
    }

    /// Core termination logic shared by `handle_fe_terminated` and `shutdown`.
    ///
    /// Cancels all allocations, stops the health checker, kills the process,
    /// and transitions to Terminated. When `drain_runnable` is true, runnable
    /// allocations are routed through finalization (needed when the FE crashes
    /// so blob handles are cleaned up).
    async fn terminate(&mut self, reason: FunctionExecutorTerminationReason, drain_runnable: bool) {
        // Cancel the health checker immediately
        if let Some(token) = &self.fe_process_cancel_token {
            token.cancel();
        }

        // Cancel all allocation tokens
        self.tracker.cancel_all_tokens();

        // Drain runnable allocations through finalization so blob handles are
        // cleaned up. Only needed on FE crash — on graceful shutdown, Running
        // allocations will see cancellation and finalize themselves.
        if drain_runnable {
            let runnable_ids = self.tracker.drain_runnable();
            let mut runnable_items: Vec<(String, ServerAllocationResult, FinalizationContext)> =
                Vec::new();
            for alloc_id in &runnable_ids {
                if let Some(info) = self.tracker.get_mut(alloc_id) &&
                    matches!(info.phase, AllocationPhase::Runnable { .. })
                {
                    let result = proto_convert::make_failure_result(
                        &info.allocation,
                        AllocationFailureReason::FunctionExecutorTerminated,
                    );
                    let ctx = info.take_finalization_ctx();
                    runnable_items.push((alloc_id.clone(), result, ctx));
                }
            }
            for (alloc_id, result, ctx) in runnable_items {
                self.start_finalization(&alloc_id, result, ctx);
            }
        }

        // Kill process
        if let Some(handle) = &self.handle {
            let _ = self.config.driver.kill(handle).await;
        }
        self.transition_to_terminated(reason);
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

/// Execute a future while recording latency, decrementing an in-progress
/// counter, and optionally incrementing an error counter.
async fn timed_phase<T>(
    latency: &opentelemetry::metrics::Histogram<f64>,
    in_progress: &opentelemetry::metrics::UpDownCounter<i64>,
    errors: Option<&opentelemetry::metrics::Counter<u64>>,
    fut: impl std::future::Future<Output = T>,
    is_err: impl FnOnce(&T) -> bool,
) -> T {
    let start = Instant::now();
    let result = fut.await;
    latency.record(start.elapsed().as_secs_f64(), &[]);
    in_progress.add(-1, &[]);
    if let Some(counter) = errors &&
        is_err(&result)
    {
        counter.add(1, &[]);
    }
    result
}

/// Record allocation outcome metrics.
fn record_allocation_metrics(
    result: &ServerAllocationResult,
    counters: &crate::metrics::DataplaneCounters,
) {
    let outcome_code = result.outcome_code.unwrap_or(0);
    let outcome = if outcome_code == AllocationOutcomeCode::Success as i32 {
        "success"
    } else {
        "failure"
    };

    let failure_reason = if outcome == "failure" {
        result.failure_reason.and_then(|r| {
            AllocationFailureReason::try_from(r)
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
