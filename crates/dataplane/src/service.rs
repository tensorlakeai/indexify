use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use proto_api::executor_api_pb::{
    AllocationLogEntry,
    ExecutorStatus,
    HostResources,
    executor_api_client::ExecutorApiClient,
};
use tokio::{
    sync::{Mutex, Notify, mpsc, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::Instrument;

#[cfg(feature = "firecracker")]
use crate::driver::FirecrackerDriver;
use crate::{
    allocation_result_dispatcher::AllocationResultDispatcher,
    blob_ops::BlobStore,
    code_cache::CodeCache,
    config::{DataplaneConfig, DriverConfig},
    driver::{DockerDriver, ForkExecDriver, ProcessDriver},
    function_container_manager::{DefaultImageResolver, FunctionContainerManager, ImageResolver},
    function_executor::controller::FESpawnConfig,
    http_proxy::run_http_proxy,
    metrics::DataplaneMetrics,
    monitoring::{MonitoringState, run_monitoring_server},
    resources::{probe_free_resources, probe_host_resources},
    secrets::{NoopSecretsProvider, SecretsProvider},
    snapshotter::Snapshotter,
    state_file::StateFile,
    state_reconciler::StateReconciler,
    state_reporter::StateReporter,
    validation,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Explicit connection state for the dataplane's relationship with the server.
///
/// Replaces the implicit `heartbeat_watch: watch::Sender<bool>` with a typed
/// enum that makes valid states and transitions visible and logged.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConnectionState {
    /// Initial state. Heartbeat running, waiting for server to accept
    /// registration. Poll loops should not start yet.
    Registering,
    /// Server accepted registration (send_state=false).
    /// Poll loops can connect. Heartbeat continues for liveness.
    Ready,
    /// Heartbeat failed. Poll loops will also fail and reconnect on
    /// their own. This state is for observability.
    Unhealthy,
}

/// Heartbeat retry backoff parameters. Keep these low so the dataplane
/// recovers quickly from transient network partitions.
const HEARTBEAT_MIN_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const HEARTBEAT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_BACKOFF_MULTIPLIER: u32 = 2;

pub struct Service {
    config: DataplaneConfig,
    channel: Channel,
    host_resources: HostResources,
    /// LVM volume group name for disk metrics (None when not using
    /// Firecracker).
    lvm_vg_name: Option<String>,
    allowed_functions: Vec<proto_api::executor_api_pb::AllowedFunction>,
    container_manager: Arc<FunctionContainerManager>,
    metrics: Arc<DataplaneMetrics>,
    state_reconciler: Arc<Mutex<StateReconciler>>,
    state_reporter: Arc<StateReporter>,
    monitoring_state: Arc<MonitoringState>,
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
}

impl Service {
    /// Create a new dataplane service with default (no-op) providers.
    pub async fn new(config: DataplaneConfig) -> Result<Self> {
        let metrics = Arc::new(DataplaneMetrics::new());
        let image_resolver: Arc<dyn ImageResolver> = Arc::new(DefaultImageResolver::new(
            config.default_function_image.clone(),
        ));
        let secrets_provider: Arc<dyn SecretsProvider> = Arc::new(NoopSecretsProvider::new());
        Self::with_providers(config, image_resolver, secrets_provider, metrics).await
    }

    /// Create a new dataplane service with custom image resolver and secrets
    /// provider.
    ///
    /// This is the extension point used by custom binaries (e.g.
    /// compute-engine-internal) to inject platform-specific implementations.
    /// The `metrics` instance is shared with the providers so all telemetry
    /// flows into the same OTel instruments.
    pub async fn with_providers(
        config: DataplaneConfig,
        image_resolver: Arc<dyn ImageResolver>,
        secrets_provider: Arc<dyn SecretsProvider>,
        metrics: Arc<DataplaneMetrics>,
    ) -> Result<Self> {
        let channel = create_channel(&config).await?;
        let discovered_gpus = crate::gpu_allocator::discover_gpus();

        // Extract LVM VG name from Firecracker driver config (if present)
        // for LVM-based disk reporting.
        let lvm_vg_name: Option<String> = {
            #[cfg(feature = "firecracker")]
            {
                match &config.sandbox_driver {
                    DriverConfig::Firecracker {
                        lvm_volume_group, ..
                    } => Some(lvm_volume_group.clone()),
                    _ => None,
                }
            }
            #[cfg(not(feature = "firecracker"))]
            {
                None
            }
        };

        let mut host_resources = probe_host_resources(&discovered_gpus, lvm_vg_name.as_deref());

        // Apply resource overrides from config.
        if let Some(overrides) = &config.resource_overrides {
            if let Some(cpu) = overrides.cpu_count {
                host_resources.cpu_count = Some(cpu);
            }
            if let Some(mem) = overrides.memory_bytes {
                host_resources.memory_bytes = Some(mem);
            }
            if let Some(disk) = overrides.disk_bytes {
                host_resources.disk_bytes = Some(disk);
            }
            tracing::info!(
                cpu_count = ?host_resources.cpu_count,
                memory_bytes = ?host_resources.memory_bytes,
                disk_bytes = ?host_resources.disk_bytes,
                "Applied resource overrides from config"
            );
        }

        tracing::info!(
            cpu_count = ?host_resources.cpu_count,
            memory_bytes = ?host_resources.memory_bytes,
            disk_bytes = ?host_resources.disk_bytes,
            gpu_count = discovered_gpus.len(),
            gpu_model = ?host_resources.gpu.as_ref().and_then(|g| g.model),
            "Host resources discovered"
        );

        let function_driver = create_process_driver(
            &config.function_driver,
            #[cfg(feature = "firecracker")]
            &config,
        )?;
        let sandbox_driver = create_process_driver(
            &config.sandbox_driver,
            #[cfg(feature = "firecracker")]
            &config,
        )?;

        tracing::info!(
            function_driver = ?config.function_driver,
            sandbox_driver = ?config.sandbox_driver,
            "Process drivers configured"
        );

        let (result_tx, result_rx) = mpsc::unbounded_channel(); // CommandResponse (acks + container events)
        let (container_state_tx, container_state_rx) = mpsc::unbounded_channel(); // CommandResponse (ContainerTerminated)
        let (activity_tx, activity_rx) = mpsc::unbounded_channel::<AllocationLogEntry>(); // AllocationLogEntry (CallFunction)
        let (outcome_tx, outcome_rx) = mpsc::unbounded_channel(); // AllocationOutcome (Completed/Failed, guaranteed delivery)

        // Ensure the state directory exists
        std::fs::create_dir_all(&config.state_dir).context("Failed to create state directory")?;

        let state_file = Arc::new(
            StateFile::new(&config.state_file_path())
                .await
                .context("Failed to initialize state file")?,
        );

        // Single blob store shared across snapshotter, code cache, and
        // allocation execution. Dispatches to local FS or S3 based on each
        // URI's scheme; clones share the same lazily-initialized S3 client.
        let blob_store = Arc::new(BlobStore::new(metrics.clone()));

        let snapshotter: Option<Arc<dyn Snapshotter>> = match &config.sandbox_driver {
            DriverConfig::Docker {
                address,
                runtime,
                runsc_root,
                snapshot_local_dir,
                ..
            } => {
                let docker = match address {
                    Some(addr) => {
                        if addr.starts_with("http://") || addr.starts_with("tcp://") {
                            bollard::Docker::connect_with_http_defaults()
                        } else {
                            bollard::Docker::connect_with_unix(
                                addr,
                                120,
                                bollard::API_DEFAULT_VERSION,
                            )
                        }
                    }
                    None => bollard::Docker::connect_with_local_defaults(),
                }
                .context("Failed to connect to Docker for snapshotter")?;
                tracing::info!("Snapshotter enabled (Docker sandbox driver)");
                let snapshotter = crate::snapshotter::docker_snapshotter::DockerSnapshotter::new(
                    docker,
                    (*blob_store).clone(),
                    metrics.clone(),
                    runtime.clone(),
                    runsc_root.clone(),
                    snapshot_local_dir.clone(),
                );
                snapshotter.cleanup_stale_overlays().await;
                Some(Arc::new(snapshotter))
            }
            #[cfg(feature = "firecracker")]
            DriverConfig::Firecracker { .. } => {
                let state_dir_path = config.firecracker_state_dir(&config.sandbox_driver);
                tracing::info!("Snapshotter enabled (Firecracker sandbox driver)");
                Some(Arc::new(
                    crate::snapshotter::firecracker_snapshotter::FirecrackerSnapshotter::new(
                        PathBuf::from(state_dir_path),
                        (*blob_store).clone(),
                        metrics.clone(),
                    ),
                ))
            }
            _ => None,
        };

        let container_manager = Arc::new(FunctionContainerManager::new(
            sandbox_driver.clone(),
            image_resolver.clone(),
            secrets_provider.clone(),
            metrics.clone(),
            state_file.clone(),
            config.executor_id.clone(),
            container_state_tx.clone(),
            snapshotter,
        ));
        let code_cache = Arc::new(CodeCache::new(
            PathBuf::from(&config.function_executor.code_cache_path),
            blob_store.clone(),
            metrics.clone(),
        ));
        let state_change_notify = Arc::new(Notify::new());

        let gpu_allocator = Arc::new(crate::gpu_allocator::GpuAllocator::new(discovered_gpus));

        let spawn_config = FESpawnConfig {
            driver: function_driver.clone(),
            image_resolver,
            gpu_allocator,
            secrets_provider,
            result_tx,
            container_state_tx,
            outcome_tx,
            activity_tx,
            server_channel: channel.clone(),
            blob_store,
            code_cache,
            executor_id: config.executor_id.clone(),
            fe_binary_path: config
                .function_executor
                .fe_binary_path
                .clone()
                .unwrap_or_else(|| "function-executor".to_string()),
            metrics: metrics.clone(),
            state_file: state_file.clone(),
        };

        let allocation_result_dispatcher = AllocationResultDispatcher::new();

        let cancel_token = CancellationToken::new();
        let state_reconciler = Arc::new(Mutex::new(StateReconciler::new(
            container_manager.clone(),
            spawn_config,
            cancel_token,
            state_change_notify.clone(),
            allocation_result_dispatcher.clone(),
        )));

        let state_reporter = Arc::new(StateReporter::new(
            result_rx,
            container_state_rx,
            activity_rx,
            outcome_rx,
        ));

        let allowed_functions = config.parse_allowed_functions();
        if !allowed_functions.is_empty() {
            tracing::info!(
                count = allowed_functions.len(),
                "Function allowlist configured"
            );
        }

        let monitoring_state = Arc::new(MonitoringState::new(Arc::new(AtomicBool::new(false))));

        Ok(Self {
            config,
            channel,
            host_resources,
            lvm_vg_name,
            allowed_functions,
            container_manager,
            metrics,
            state_reconciler,
            state_reporter,
            monitoring_state,
            allocation_result_dispatcher,
        })
    }

    pub async fn run(self) -> Result<()> {
        let executor_id = self.config.executor_id.clone();
        tracing::info!("Starting dataplane service");

        // Capture the current span (from start_dataplane's #[instrument]) which
        // contains executor_id. We'll propagate it to all spawned tasks so every
        // log line includes executor_id.
        let span = tracing::Span::current();

        // Recover containers from previous run (FCM sandbox path)
        let recovered = self.container_manager.recover().await;
        if recovered > 0 {
            tracing::info!(recovered, "Recovered FCM containers from state file");
        }

        // Recover containers from previous run (AC function path)
        let ac_known_handles = self
            .state_reconciler
            .lock()
            .await
            .recover_ac_containers()
            .await;
        if !ac_known_handles.is_empty() {
            tracing::info!(
                recovered = ac_known_handles.len(),
                "Recovered AC containers from state file"
            );
        }

        // Clean up orphaned containers (exist in Docker but not in state file).
        // Both FCM and AC recovered handles are excluded from cleanup.
        let cleaned = self
            .container_manager
            .cleanup_orphans(&ac_known_handles)
            .await;
        if cleaned > 0 {
            tracing::info!(cleaned, "Cleaned up orphaned containers");
        }

        let cancel_token = CancellationToken::new();
        let heartbeat_healthy = self.monitoring_state.heartbeat_healthy.clone();
        let (connection_state_tx, _) = watch::channel(ConnectionState::Registering);

        let runtime = Arc::new(ServiceRuntime {
            channel: self.channel.clone(),
            identity: ExecutorIdentity {
                executor_id: executor_id.clone(),
                host_resources: self.host_resources,
                allowed_functions: self.allowed_functions.clone(),
                labels: self.config.labels.clone(),
                proxy_address: self.config.http_proxy.get_advertise_address(),
                server_addr: self.config.server_addr.clone(),
            },
            state_reconciler: self.state_reconciler.clone(),
            state_reporter: self.state_reporter.clone(),
            heartbeat_healthy: heartbeat_healthy.clone(),
            connection_state: connection_state_tx,
            cancel_token: cancel_token.clone(),
            metrics: self.metrics.clone(),
            monitoring_state: self.monitoring_state.clone(),
            last_applied_command_seq: Arc::new(AtomicU64::new(0)),
            last_applied_result_seq: Arc::new(AtomicU64::new(0)),
            allocation_result_dispatcher: self.allocation_result_dispatcher.clone(),
        });

        let mut tasks = JoinSet::new();

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_heartbeat_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_poll_commands_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_poll_results_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let container_manager = self.container_manager.clone();
            let cancel_token = cancel_token.clone();
            async move { container_manager.run_health_checks(cancel_token).await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let metrics = self.metrics.clone();
            let lvm_vg_name = self.lvm_vg_name.clone();
            let cancel_token = cancel_token.clone();
            async move { run_metrics_update_loop(metrics, lvm_vg_name, cancel_token).await }
                .instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let cancel_token = cancel_token.clone();
            let http_proxy_config = self.config.http_proxy.clone();
            let container_manager = self.container_manager.clone();
            let executor_id = executor_id.clone();
            async move {
                if let Err(e) = run_http_proxy(
                    http_proxy_config,
                    container_manager,
                    executor_id,
                    cancel_token,
                )
                .await
                {
                    tracing::error!(error = ?e, "HTTP proxy server error");
                }
            }
            .instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let cancel_token = cancel_token.clone();
            let monitoring_state = self.monitoring_state.clone();
            let monitoring_addr = self.config.monitoring.socket_addr();
            async move {
                run_monitoring_server(&monitoring_addr, monitoring_state, cancel_token).await;
            }
            .instrument(span)
        });

        tokio::select! {
            signal_name = wait_for_shutdown_signal() => {
                tracing::info!(signal = signal_name, "Shutdown signal received");
            }
            Some(result) = tasks.join_next() => {
                match result {
                    Ok(()) => tracing::warn!("Background task exited unexpectedly"),
                    Err(e) => tracing::error!(error = ?e, "Background task panicked"),
                }
            }
        }

        // Always cancel all tasks and clean up, regardless of which branch triggered.
        cancel_token.cancel();

        // Graceful shutdown with force-exit fallback.
        // After cancel_token fires, background tasks may still hold the
        // state_reconciler lock while winding down.  A second Ctrl+C or a
        // 30-second timeout lets the operator force-exit instead of hanging
        // indefinitely.
        tokio::select! {
            _ = async {
                self.state_reconciler.lock().await.shutdown().await;
            } => {
                tracing::info!("Graceful shutdown completed");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("Second interrupt received during shutdown, forcing exit");
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                tracing::warn!("Shutdown timed out after 30s, forcing exit");
            }
        }

        Ok(())
    }
}

const METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(5);

/// Periodically update resource availability metrics.
async fn run_metrics_update_loop(
    metrics: Arc<DataplaneMetrics>,
    lvm_vg_name: Option<String>,
    cancel_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(METRICS_UPDATE_INTERVAL);
    // Reuse a single System instance across ticks so that sysinfo can compute
    // cpu_usage() as a delta between consecutive refreshes.  A fresh System on
    // every tick would have no baseline and always report 0% CPU usage.
    let mut sys = sysinfo::System::new();

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Metrics update loop cancelled");
                return;
            }
            _ = interval.tick() => {
                let resources = probe_free_resources(&mut sys, lvm_vg_name.as_deref());
                metrics.update_resources(resources).await;
            }
        }
    }
}

/// Static identity fields for this executor, used in heartbeats and poll
/// requests.
struct ExecutorIdentity {
    executor_id: String,
    host_resources: HostResources,
    allowed_functions: Vec<proto_api::executor_api_pb::AllowedFunction>,
    labels: std::collections::HashMap<String, String>,
    proxy_address: String,
    server_addr: String,
}

/// Shared runtime context for heartbeat and poll loops.
struct ServiceRuntime {
    channel: Channel,
    identity: ExecutorIdentity,
    state_reconciler: Arc<Mutex<StateReconciler>>,
    state_reporter: Arc<StateReporter>,
    heartbeat_healthy: Arc<AtomicBool>,
    /// Explicit connection state machine. Replaces the old `heartbeat_watch:
    /// watch::Sender<bool>`. Transitions are logged in the heartbeat loop;
    /// poll loops observe via `subscribe()`.
    connection_state: watch::Sender<ConnectionState>,
    cancel_token: CancellationToken,
    metrics: Arc<DataplaneMetrics>,
    monitoring_state: Arc<MonitoringState>,
    /// Highest command seq successfully applied (used by poll_commands ack).
    last_applied_command_seq: Arc<AtomicU64>,
    /// Highest result seq successfully applied (used by poll_allocation_results
    /// ack).
    last_applied_result_seq: Arc<AtomicU64>,
    /// Dispatcher for routing allocation results to allocation runners.
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
}

impl ServiceRuntime {
    /// Transition the connection state if it differs from the current state.
    /// Logs the transition with executor ID, old/new state, and reason.
    /// No-op (no log) if already in the target state.
    fn transition_connection_state(&self, to: ConnectionState, reason: &str) {
        let old = *self.connection_state.borrow();
        if old != to {
            self.connection_state.send(to).ok();
            tracing::info!(
                executor_id = %self.identity.executor_id,
                from = ?old,
                to = ?to,
                reason,
                "Connection state transition"
            );
        }
    }

    /// Wait for the connection state to become `Ready`. Returns `true` if
    /// ready, `false` if the channel closed (should exit the loop).
    async fn wait_for_ready(&self, loop_name: &str) -> bool {
        let mut state_rx = self.connection_state.subscribe();
        if state_rx
            .wait_for(|s| *s == ConnectionState::Ready)
            .await
            .is_err()
        {
            tracing::info!(
                executor_id = %self.identity.executor_id,
                loop_name,
                "Connection state channel closed, exiting poll loop"
            );
            return false;
        }
        tracing::info!(
            executor_id = %self.identity.executor_id,
            loop_name,
            "Connection ready, starting poll loop"
        );
        true
    }

    /// Build a full state sync message (identity + all containers).
    fn build_full_state(
        &self,
        fe_states: &[proto_api::executor_api_pb::ContainerState],
    ) -> proto_api::executor_api_pb::DataplaneStateFullSync {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "localhost".to_string());
        proto_api::executor_api_pb::DataplaneStateFullSync {
            hostname: Some(hostname),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            total_resources: Some(self.identity.host_resources),
            total_container_resources: Some(self.identity.host_resources),
            allowed_functions: self.identity.allowed_functions.clone(),
            labels: self.identity.labels.clone(),
            catalog_entry_name: None,
            proxy_address: Some(self.identity.proxy_address.clone()),
            container_states: fe_states.to_vec(),
        }
    }

    async fn run_heartbeat_loop(&self) {
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let mut retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;
        let mut send_full_state = false; // First heartbeat has no state; server asks for it

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Heartbeat loop cancelled");
                return;
            }

            // Phase 1: Build and send heartbeat(s) (includes batched reports).
            // If buffers exceed the 10MB fragmentation limit, loop sending
            // additional heartbeats until all buffers are drained.
            let report_start = std::time::Instant::now();

            // Build full_state payload if the server requested it
            let full_state = if send_full_state {
                tracing::info!("Sending full state in heartbeat");
                let reconciler_guard = self.state_reconciler.lock().await;
                let fe_states = reconciler_guard.get_all_fe_states().await;
                drop(reconciler_guard);
                Some(self.build_full_state(&fe_states))
            } else {
                None
            };

            let mut heartbeat_failed = false;
            let mut is_first_fragment = true;

            loop {
                // Track cumulative message size across collect calls so
                // each buffer respects the 10MB total limit.
                let mut base_size: usize = 0;

                let (responses, resp_count, resp_remaining) =
                    self.state_reporter.collect_responses(base_size).await;
                base_size += responses
                    .iter()
                    .map(prost::Message::encoded_len)
                    .sum::<usize>();

                let (outcomes, out_count, out_remaining) =
                    self.state_reporter.collect_outcomes(base_size).await;
                base_size += outcomes
                    .iter()
                    .map(prost::Message::encoded_len)
                    .sum::<usize>();

                let (log_entries, log_count, log_remaining) =
                    self.state_reporter.collect_log_entries(base_size).await;

                let heartbeat_req = proto_api::executor_api_pb::HeartbeatRequest {
                    executor_id: Some(self.identity.executor_id.clone()),
                    status: Some(ExecutorStatus::Running.into()),
                    // Only include full_state in the first fragment
                    full_state: if is_first_fragment {
                        full_state.clone()
                    } else {
                        None
                    },
                    command_responses: responses,
                    allocation_outcomes: outcomes,
                    allocation_log_entries: log_entries,
                };

                let request_size = prost::Message::encoded_len(&heartbeat_req);
                self.metrics.counters.state_report_rpcs.add(1, &[]);
                self.metrics
                    .histograms
                    .state_report_message_size_mb
                    .record(request_size as f64 / (1024.0 * 1024.0), &[]);

                if is_first_fragment {
                    // Store reported state for monitoring endpoint
                    *self.monitoring_state.last_reported_state.lock().await =
                        Some(format!("{:#?}", heartbeat_req));
                }

                let rpc_result = tokio::select! {
                    _ = self.cancel_token.cancelled() => return,
                    result = client.heartbeat(heartbeat_req) => result,
                };
                match rpc_result {
                    Ok(response) => {
                        self.metrics
                            .histograms
                            .state_report_rpc_latency_seconds
                            .record(report_start.elapsed().as_secs_f64(), &[]);
                        self.metrics.counters.record_heartbeat(true);
                        retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;

                        // Any successful RPC means the server is reachable.
                        self.monitoring_state.ready.store(true, Ordering::SeqCst);

                        // Reset seq counters whenever we sent full_state. The
                        // server creates a fresh ExecutorConnection with new
                        // command/result buffers on re-registration, so old
                        // acked_seq values would incorrectly drain new data.
                        if is_first_fragment && full_state.is_some() {
                            self.last_applied_command_seq.store(0, Ordering::SeqCst);
                            self.last_applied_result_seq.store(0, Ordering::SeqCst);
                        }

                        // Check send_state BEFORE draining buffers. When the
                        // server doesn't recognize this executor it drops all
                        // reports and responds with send_state=true. We must
                        // keep buffered items so they are resent on the next
                        // heartbeat (which will include full_state).
                        let server_needs_state = if is_first_fragment {
                            let resp = response.into_inner();
                            resp.send_state.unwrap_or(false)
                        } else {
                            false
                        };

                        if server_needs_state {
                            // Server didn't process our reports — retain them.
                            tracing::info!(
                                resp_count,
                                out_count,
                                log_count,
                                "Server requested full state, retaining buffered reports"
                            );
                            send_full_state = true;
                            self.transition_connection_state(
                                ConnectionState::Registering,
                                "server requested re-registration",
                            );
                            break;
                        }

                        send_full_state = false;

                        // Server accepted our reports — safe to drain.
                        self.state_reporter.drain_sent(resp_count).await;
                        self.state_reporter.drain_sent_outcomes(out_count).await;
                        self.state_reporter.drain_sent_log_entries(log_count).await;

                        if is_first_fragment {
                            self.heartbeat_healthy.store(true, Ordering::SeqCst);
                            self.transition_connection_state(
                                ConnectionState::Ready,
                                "server accepted registration",
                            );
                        }
                    }
                    Err(e) => {
                        self.metrics.counters.record_heartbeat(false);
                        self.metrics.counters.state_report_rpc_errors.add(1, &[]);
                        self.metrics
                            .histograms
                            .state_report_rpc_latency_seconds
                            .record(report_start.elapsed().as_secs_f64(), &[]);
                        tracing::warn!(
                            error = ?e,
                            server_addr = %self.identity.server_addr,
                            retry_in_secs = retry_interval.as_secs(),
                            "Heartbeat failed, retrying with backoff"
                        );
                        self.heartbeat_healthy.store(false, Ordering::SeqCst);
                        self.transition_connection_state(
                            ConnectionState::Unhealthy,
                            "heartbeat RPC failed",
                        );
                        // Don't set send_full_state here. The server will
                        // request it via send_state=true once connectivity
                        // is restored and the server sees an unknown executor.
                        heartbeat_failed = true;
                        break;
                    }
                }

                is_first_fragment = false;

                // If all buffers are fully drained, we're done
                if !resp_remaining && !out_remaining && !log_remaining {
                    break;
                }
            }

            if heartbeat_failed {
                tokio::select! {
                    _ = self.cancel_token.cancelled() => return,
                    _ = tokio::time::sleep(retry_interval) => {}
                }
                retry_interval = std::cmp::min(
                    retry_interval * HEARTBEAT_BACKOFF_MULTIPLIER,
                    HEARTBEAT_MAX_RETRY_INTERVAL,
                );
                continue;
            }

            // Phase 2: Wait for next trigger
            // If the server requested full state, send it immediately without
            // waiting for the heartbeat interval.
            if send_full_state {
                continue;
            }

            let results_notify = self.state_reporter.results_notify();
            let outcomes_notify = self.state_reporter.outcomes_notify();
            let activities_notify = self.state_reporter.activities_notify();
            let reconciler = self.state_reconciler.lock().await;
            let state_change_notify = reconciler.state_change_notify();
            drop(reconciler);

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Heartbeat loop cancelled");
                    return;
                }
                _ = results_notify.notified() => {
                    // Command responses available, send immediately
                }
                _ = outcomes_notify.notified() => {
                    // Allocation outcomes available, send immediately
                }
                _ = activities_notify.notified() => {
                    // Allocation log entries available, send immediately
                }
                _ = state_change_notify.notified() => {
                    // FE added/removed, send immediately
                }
                _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
            }
        }
    }

    // ---------------------------------------------------------------
    // Poll commands loop (replaces command stream)
    // ---------------------------------------------------------------

    async fn run_poll_commands_loop(&self) {
        if !self.wait_for_ready("poll_commands").await {
            return;
        }
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let mut retry_interval = Duration::from_secs(1);

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Poll commands loop cancelled");
                return;
            }

            let acked_seq = self.last_applied_command_seq.load(Ordering::SeqCst);
            let request = proto_api::executor_api_pb::PollCommandsRequest {
                executor_id: self.identity.executor_id.clone(),
                acked_command_seq: if acked_seq > 0 { Some(acked_seq) } else { None },
            };

            let rpc_result = tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                result = client.poll_commands(request) => result,
            };

            match rpc_result {
                Ok(response) => {
                    retry_interval = Duration::from_secs(1);
                    for command in response.into_inner().commands {
                        let seq = command.seq;
                        // Skip commands already processed (re-delivery after lost ack)
                        if seq <= self.last_applied_command_seq.load(Ordering::SeqCst) {
                            continue;
                        }
                        self.handle_command(command).await;
                        self.last_applied_command_seq
                            .fetch_max(seq, Ordering::SeqCst);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "poll_commands failed");
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(retry_interval) => {},
                    }
                    retry_interval = std::cmp::min(retry_interval * 2, Duration::from_secs(10));
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Poll allocation results loop (replaces allocation stream)
    // ---------------------------------------------------------------

    async fn run_poll_results_loop(&self) {
        if !self.wait_for_ready("poll_results").await {
            return;
        }
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let mut retry_interval = Duration::from_secs(1);

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Poll results loop cancelled");
                return;
            }

            let acked_seq = self.last_applied_result_seq.load(Ordering::SeqCst);
            let request = proto_api::executor_api_pb::PollAllocationResultsRequest {
                executor_id: self.identity.executor_id.clone(),
                acked_result_seq: if acked_seq > 0 { Some(acked_seq) } else { None },
            };

            let rpc_result = tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                result = client.poll_allocation_results(request) => result,
            };

            match rpc_result {
                Ok(response) => {
                    retry_interval = Duration::from_secs(1);
                    for result in response.into_inner().results {
                        let seq = result.seq;
                        // Skip results already processed (re-delivery after lost ack)
                        if seq <= self.last_applied_result_seq.load(Ordering::SeqCst) {
                            continue;
                        }
                        if let Some(entry) = result.entry {
                            self.dispatch_function_call_result(entry);
                        }
                        self.last_applied_result_seq
                            .fetch_max(seq, Ordering::SeqCst);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "poll_allocation_results failed");
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(retry_interval) => {},
                    }
                    retry_interval = std::cmp::min(retry_interval * 2, Duration::from_secs(10));
                }
            }
        }
    }

    /// Dispatch a function call result from the poll results loop to the
    /// allocation runner handling the target allocation.
    fn dispatch_function_call_result(
        &self,
        log_entry: proto_api::executor_api_pb::AllocationLogEntry,
    ) {
        let allocation_id = log_entry.allocation_id.clone();
        let dispatcher = self.allocation_result_dispatcher.clone();
        tokio::spawn(async move {
            if !dispatcher.dispatch(&allocation_id, log_entry).await {
                tracing::warn!(
                    allocation_id = %allocation_id,
                    "No allocation runner for result dispatch"
                );
            }
        });
    }

    /// Handle a single Command from the poll commands loop.
    async fn handle_command(&self, command: proto_api::executor_api_pb::Command) {
        let seq = command.seq;

        let Some(cmd) = command.command else {
            tracing::warn!(seq, "Received command with no payload, skipping");
            return;
        };

        use proto_api::executor_api_pb::command::Command as Cmd;
        match cmd {
            Cmd::AddContainer(add) => {
                if let Some(container) = add.container {
                    if let Err(e) = validation::validate_fe_description(&container) {
                        tracing::warn!(
                            seq,
                            container_id = %container.id.as_deref().unwrap_or(""),
                            namespace = %container.function.as_ref().and_then(|f| f.namespace.as_deref()).unwrap_or(""),
                            app = %container.function.as_ref().and_then(|f| f.application_name.as_deref()).unwrap_or(""),
                            "fn" = %container.function.as_ref().and_then(|f| f.function_name.as_deref()).unwrap_or(""),
                            sandbox_id = %container.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()).unwrap_or(""),
                            pool_id = %container.pool_id.as_deref().unwrap_or(""),
                            error = %e,
                            "Skipping invalid AddContainer command"
                        );
                    } else {
                        tracing::info!(
                            seq,
                            container_id = %container.id.as_deref().unwrap_or(""),
                            namespace = %container.function.as_ref().and_then(|f| f.namespace.as_deref()).unwrap_or(""),
                            app = %container.function.as_ref().and_then(|f| f.application_name.as_deref()).unwrap_or(""),
                            "fn" = %container.function.as_ref().and_then(|f| f.function_name.as_deref()).unwrap_or(""),
                            sandbox_id = %container.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()).unwrap_or(""),
                            pool_id = %container.pool_id.as_deref().unwrap_or(""),
                            "AddContainer command"
                        );
                        let mut reconciler = self.state_reconciler.lock().await;
                        reconciler
                            .reconcile_containers(vec![container], vec![])
                            .await;
                    }
                }
            }
            Cmd::RemoveContainer(remove) => {
                tracing::info!(
                    seq,
                    container_id = %remove.container_id,
                    "RemoveContainer command"
                );
                let mut reconciler = self.state_reconciler.lock().await;
                reconciler
                    .reconcile_containers(vec![], vec![remove.container_id])
                    .await;
            }
            Cmd::RunAllocation(run) => {
                if let Some(allocation) = run.allocation {
                    if let Err(e) = validation::validate_allocation(&allocation) {
                        tracing::warn!(
                            seq,
                            allocation_id = %allocation.allocation_id.as_deref().unwrap_or(""),
                            request_id = %allocation.request_id.as_deref().unwrap_or(""),
                            container_id = %allocation.container_id.as_deref().unwrap_or(""),
                            namespace = %allocation.function.as_ref().and_then(|f| f.namespace.as_deref()).unwrap_or(""),
                            "fn" = %allocation.function.as_ref().and_then(|f| f.function_name.as_deref()).unwrap_or(""),
                            error = %e,
                            "Skipping invalid RunAllocation command"
                        );
                    } else if let Some(fe_id) = allocation.container_id.clone() {
                        tracing::info!(
                            seq,
                            allocation_id = %allocation.allocation_id.as_deref().unwrap_or(""),
                            request_id = %allocation.request_id.as_deref().unwrap_or(""),
                            container_id = %fe_id,
                            namespace = %allocation.function.as_ref().and_then(|f| f.namespace.as_deref()).unwrap_or(""),
                            "fn" = %allocation.function.as_ref().and_then(|f| f.function_name.as_deref()).unwrap_or(""),
                            "RunAllocation command"
                        );
                        let mut reconciler = self.state_reconciler.lock().await;
                        reconciler
                            .reconcile_allocations(vec![(fe_id, allocation, seq)])
                            .await;
                    } else {
                        tracing::warn!(
                            seq,
                            allocation_id = %allocation.allocation_id.as_deref().unwrap_or(""),
                            request_id = %allocation.request_id.as_deref().unwrap_or(""),
                            namespace = %allocation.function.as_ref().and_then(|f| f.namespace.as_deref()).unwrap_or(""),
                            "fn" = %allocation.function.as_ref().and_then(|f| f.function_name.as_deref()).unwrap_or(""),
                            "RunAllocation missing container_id"
                        );
                    }
                }
            }
            Cmd::KillAllocation(kill) => {
                tracing::warn!(
                    seq,
                    allocation_id = %kill.allocation_id,
                    "KillAllocation command received (not yet implemented)"
                );
            }
            // DeliverResult was removed from Command — function call results
            // are now delivered via the AllocationEvent log.
            Cmd::UpdateContainerDescription(update) => {
                tracing::info!(
                    seq,
                    container_id = %update.container_id,
                    sandbox_id = ?update.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()),
                    "UpdateContainerDescription command"
                );
                let mut reconciler = self.state_reconciler.lock().await;
                reconciler.update_container_description(update).await;
            }
            Cmd::SnapshotContainer(snapshot) => {
                tracing::info!(
                    seq,
                    container_id = %snapshot.container_id,
                    snapshot_id = %snapshot.snapshot_id,
                    upload_uri = %snapshot.upload_uri,
                    "SnapshotContainer command"
                );
                let reconciler = self.state_reconciler.lock().await;
                let container_manager = reconciler.container_manager().clone();
                drop(reconciler);
                tokio::spawn(async move {
                    container_manager
                        .snapshot_container(
                            &snapshot.container_id,
                            &snapshot.snapshot_id,
                            &snapshot.upload_uri,
                        )
                        .await;
                });
            }
        }
    }
}

/// Wait for any shutdown signal (SIGINT, SIGTERM, SIGQUIT).
/// Returns the name of the signal received.
async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        let mut sigquit = signal(SignalKind::quit()).expect("Failed to install SIGQUIT handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
            _ = sigquit.recv() => "SIGQUIT",
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c");
        "SIGINT"
    }
}

/// Create a process driver from a DriverConfig.
fn create_process_driver(
    driver_config: &DriverConfig,
    #[cfg(feature = "firecracker")] config: &DataplaneConfig,
) -> Result<Arc<dyn ProcessDriver>> {
    match driver_config {
        DriverConfig::ForkExec => Ok(Arc::new(ForkExecDriver::new())),
        DriverConfig::Docker {
            address,
            runtime,
            network,
            binds,
            ..
        } => match address {
            Some(addr) => Ok(Arc::new(DockerDriver::with_address(
                addr,
                runtime.clone(),
                network.clone(),
                binds.clone(),
            )?)),
            None => Ok(Arc::new(DockerDriver::new(
                runtime.clone(),
                network.clone(),
                binds.clone(),
            )?)),
        },
        #[cfg(feature = "firecracker")]
        DriverConfig::Firecracker {
            firecracker_binary,
            kernel_image_path,
            default_rootfs_size_bytes,
            base_rootfs_image,
            cni_network_name,
            cni_bin_path,
            guest_gateway,
            guest_netmask,
            default_vcpu_count,
            default_memory_mib,
            lvm_volume_group,
            lvm_thin_pool,
            ..
        } => Ok(Arc::new(FirecrackerDriver::new(
            firecracker_binary.clone(),
            kernel_image_path.clone(),
            *default_rootfs_size_bytes,
            base_rootfs_image.clone(),
            cni_network_name.clone(),
            cni_bin_path.clone(),
            guest_gateway.clone(),
            guest_netmask.clone(),
            *default_vcpu_count,
            *default_memory_mib,
            config.firecracker_state_dir(driver_config),
            config.firecracker_log_dir(driver_config),
            lvm_volume_group.clone(),
            lvm_thin_pool.clone(),
        )?)),
    }
}

async fn create_channel(config: &DataplaneConfig) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(config.server_addr.clone())
        .context("Invalid server address")?
        .connect_timeout(Duration::from_secs(10));

    if config.tls.enabled {
        let mut tls_config = ClientTlsConfig::new();

        if let Some(domain) = &config.tls.domain_name {
            tls_config = tls_config.domain_name(domain.clone());
        }

        if let Some(ca_path) = &config.tls.ca_cert_path {
            let ca_cert = tokio::fs::read(ca_path)
                .await
                .context("Failed to read CA certificate")?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca_cert);
            tls_config = tls_config.ca_certificate(ca_cert);
        }

        if let (Some(cert_path), Some(key_path)) =
            (&config.tls.client_cert_path, &config.tls.client_key_path)
        {
            let client_cert = tokio::fs::read(cert_path)
                .await
                .context("Failed to read client certificate")?;
            let client_key = tokio::fs::read(key_path)
                .await
                .context("Failed to read client key")?;
            let identity = tonic::transport::Identity::from_pem(client_cert, client_key);
            tls_config = tls_config.identity(identity);
        }

        endpoint = endpoint.tls_config(tls_config)?;
    }

    // Use connect_lazy() so the dataplane can start even if the server is
    // unavailable. The connection will be established when the first request is
    // made (heartbeat).
    let channel = endpoint.connect_lazy();

    tracing::info!(server_addr = %config.server_addr, "Channel created");
    Ok(channel)
}
