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
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
    FunctionExecutorType,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{Instrument, error, info, warn};

use super::{
    events::{AllocationOutcome, CompletedAllocation, FECommand, FEEvent},
    fe_client::FunctionExecutorGrpcClient,
    health_checker,
    watcher_registry::WatcherRegistry,
};
use crate::{
    blob_ops::BlobStore,
    code_cache::CodeCache,
    driver::{ProcessConfig, ProcessDriver, ProcessHandle, ProcessType},
};

/// Timeout for connecting to the FE after spawning the process.
const FE_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Handle returned to the StateReconciler for communicating with a controller.
pub struct FEControllerHandle {
    /// Send commands (new allocations, shutdown).
    pub command_tx: mpsc::UnboundedSender<FECommand>,
    /// Read current FE state for heartbeat reporting.
    pub state_rx: watch::Receiver<FunctionExecutorState>,
    /// Join handle for the controller task.
    pub join_handle: tokio::task::JoinHandle<()>,
}

/// State of an allocation tracked by the controller.
struct AllocationInfo {
    allocation: ServerAllocation,
    state: AllocationState,
    cancel_token: CancellationToken,
}

#[derive(Debug, Clone, PartialEq)]
enum AllocationState {
    Pending,
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
        driver: Arc<dyn ProcessDriver>,
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
            watcher_registry,
            fe_process_cancel_token: None,
        };

        let join_handle = tokio::spawn(
            async move {
                controller.run().await;
            }
            .instrument(tracing::info_span!("fe_controller", fe_id = %fe_id)),
        );

        FEControllerHandle {
            command_tx,
            state_rx,
            join_handle,
        }
    }

    /// Main control loop.
    async fn run(&mut self) {
        // Phase 1: Start the FE process
        info!(fe_id = ?self.description.id, "Starting function executor");
        if let Err(e) = self.start_fe_process().await {
            error!(error = %e, "Failed to start function executor process");
            self.transition_to_terminated(
                FunctionExecutorTerminationReason::StartupFailedInternalError,
            );
            return;
        }

        // Phase 2: Connect to FE and initialize
        match self.connect_and_initialize().await {
            Ok(client) => {
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
                error!(error = %e, "Failed to connect/initialize function executor");
                // Kill the process since we can't use it
                if let Some(handle) = &self.handle {
                    let _ = self.driver.kill(handle).await;
                }
                self.transition_to_terminated(
                    FunctionExecutorTerminationReason::StartupFailedInternalError,
                );
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
                        FECommand::RemoveAllocation(id) => self.handle_remove_allocation(&id),
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

        // Build environment variables
        let mut env = vec![];
        // Pass executor ID and FE ID as env vars (FE may need them)
        env.push(("INDEXIFY_EXECUTOR_ID".to_string(), self.executor_id.clone()));
        env.push(("INDEXIFY_FE_ID".to_string(), fe_id.clone()));

        let config =
            ProcessConfig {
                id: fe_id.clone(),
                process_type: ProcessType::Function,
                image: None, // ForkExec mode, no image needed
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

        let mut client =
            FunctionExecutorGrpcClient::connect_with_retry(addr, FE_READY_TIMEOUT).await?;

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

        let init_response = client.initialize(init_request).await.map_err(|e| {
            error!(error = %e, "FE initialize RPC call failed");
            e
        })?;

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
        // If we're terminated/terminating, don't accept new allocations
        if let Some(status) = self.state_tx.borrow().status {
            if status == FunctionExecutorStatus::Terminated as i32 {
                warn!(
                    allocation_id = ?allocation.allocation_id,
                    "Rejecting allocation, FE is terminated"
                );
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
                        failure_reason: Some(
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated
                                .into(),
                        ),
                        return_value: None,
                        request_error: None,
                        execution_duration_ms: None,
                    };
                    let _ = self.result_tx.send(CompletedAllocation { result });
                }
                return;
            }
        }

        let alloc_id = allocation.allocation_id.clone().unwrap_or_default();

        if self.allocations.contains_key(&alloc_id) {
            return; // Already tracking
        }

        let alloc_cancel = self.cancel_token.child_token();
        let info = AllocationInfo {
            allocation,
            state: AllocationState::Runnable,
            cancel_token: alloc_cancel,
        };

        self.allocations.insert(alloc_id.clone(), info);
        self.runnable_queue.push_back(alloc_id);
        self.schedule_allocations();
    }

    fn handle_remove_allocation(&mut self, allocation_id: &str) {
        if let Some(info) = self.allocations.get(allocation_id) {
            info.cancel_token.cancel();
        }
        // Remove from runnable queue if not yet started
        self.runnable_queue.retain(|id| id != allocation_id);
    }

    fn schedule_allocations(&mut self) {
        while self.running_set.len() < self.max_concurrency as usize {
            let Some(alloc_id) = self.runnable_queue.pop_front() else {
                break;
            };

            let Some(info) = self.allocations.get_mut(&alloc_id) else {
                continue;
            };

            if info.cancel_token.is_cancelled() {
                continue;
            }

            info.state = AllocationState::Running;
            self.running_set.insert(alloc_id.clone());

            // Spawn allocation execution
            // We need the client to be available. If terminated, client is None.
            let Some(client) = self.client.clone() else {
                warn!(
                    allocation_id = %alloc_id,
                    "Cannot schedule allocation, FE client is not available (terminated?)"
                );
                // We should probably fail the allocation here.
                // If we are here, info.state is Running.
                self.running_set.remove(&alloc_id);
                if let Some(info) = self.allocations.get_mut(&alloc_id) {
                    info.state = AllocationState::Done;
                }

                // Report failure
                if let Some(info) = self.allocations.get(&alloc_id) {
                    let result = proto_api::executor_api_pb::AllocationResult {
                        function: info.allocation.function.clone(),
                        allocation_id: info.allocation.allocation_id.clone(),
                        function_call_id: info.allocation.function_call_id.clone(),
                        request_id: info.allocation.request_id.clone(),
                        outcome_code: Some(
                            proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into(),
                        ),
                        failure_reason: Some(
                            proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated
                                .into(),
                        ),
                        return_value: None,
                        request_error: None,
                        execution_duration_ms: None,
                    };
                    let _ = self.result_tx.send(CompletedAllocation { result });
                }
                continue;
            };

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
                let result = super::allocation_runner::run_allocation(
                    client,
                    allocation,
                    server_channel,
                    blob_store,
                    allocation_timeout,
                    cancel_token,
                    watcher_registry,
                )
                .await;

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
                if Some(fe_id) != self.description.id {
                    return false;
                }
                warn!(reason = ?reason, "Function executor terminated");
                // Cancel all running allocations
                for info in self.allocations.values() {
                    info.cancel_token.cancel();
                }
                // Report failed results for all pending/running allocations
                self.report_all_allocations_failed(reason);
                // Kill process
                if let Some(handle) = &self.handle {
                    let _ = self.driver.kill(handle).await;
                }
                self.transition_to_terminated(reason);
                return false; // don't exit, stay in loop to report status until removed by server
            }
            FEEvent::AllocationExecutionFinished {
                allocation_id,
                result,
            } => {
                self.running_set.remove(&allocation_id);
                if let Some(info) = self.allocations.get_mut(&allocation_id) {
                    info.state = AllocationState::Done;
                }

                // Convert outcome to server AllocationResult
                let alloc = self.allocations.get(&allocation_id).map(|i| &i.allocation);

                if let Some(alloc) = alloc {
                    let server_result = match result {
                        AllocationOutcome::Completed {
                            result,
                            execution_duration_ms: _,
                            fe_result: _,
                        } => result,
                        AllocationOutcome::Cancelled => {
                            proto_api::executor_api_pb::AllocationResult {
                                function: alloc.function.clone(),
                                allocation_id: alloc.allocation_id.clone(),
                                function_call_id: alloc.function_call_id.clone(),
                                request_id: alloc.request_id.clone(),
                                outcome_code: Some(
                                    proto_api::executor_api_pb::AllocationOutcomeCode::Failure
                                        .into(),
                                ),
                                failure_reason: Some(
                                    proto_api::executor_api_pb::AllocationFailureReason::AllocationCancelled.into(),
                                ),
                                return_value: None,
                                request_error: None,
                                execution_duration_ms: None,
                            }
                        }
                        AllocationOutcome::Failed {
                            reason,
                            error_message: _,
                        } => proto_api::executor_api_pb::AllocationResult {
                            function: alloc.function.clone(),
                            allocation_id: alloc.allocation_id.clone(),
                            function_call_id: alloc.function_call_id.clone(),
                            request_id: alloc.request_id.clone(),
                            outcome_code: Some(
                                proto_api::executor_api_pb::AllocationOutcomeCode::Failure.into(),
                            ),
                            failure_reason: Some(reason.into()),
                            return_value: None,
                            request_error: None,
                            execution_duration_ms: None,
                        },
                    };

                    let _ = self.result_tx.send(CompletedAllocation {
                        result: server_result,
                    });
                }

                // Schedule more allocations
                self.schedule_allocations();
            }
            FEEvent::FunctionExecutorCreated(_) |
            FEEvent::AllocationPreparationFinished { .. } |
            FEEvent::ScheduleAllocationExecution |
            FEEvent::AllocationFinalizationFinished { .. } => {
                // Handle in future phases
            }
        }
        false // don't exit
    }

    async fn shutdown(&mut self) {
        info!("Shutting down function executor controller");
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

    fn report_all_allocations_failed(&self, termination_reason: FunctionExecutorTerminationReason) {
        let failure_reason = match termination_reason {
            FunctionExecutorTerminationReason::Unhealthy |
            FunctionExecutorTerminationReason::InternalError => {
                proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated
            }
            FunctionExecutorTerminationReason::FunctionTimeout => {
                proto_api::executor_api_pb::AllocationFailureReason::FunctionTimeout
            }
            FunctionExecutorTerminationReason::Oom => {
                proto_api::executor_api_pb::AllocationFailureReason::Oom
            }
            _ => proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated,
        };

        for (_, info) in &self.allocations {
            if info.state != AllocationState::Done {
                let result = proto_api::executor_api_pb::AllocationResult {
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
                let _ = self.result_tx.send(CompletedAllocation { result });
            }
        }
    }
}
