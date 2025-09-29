use std::{sync::Arc, time::Duration};

use sha2::{Digest, Sha256};
use tokio::{
    signal,
    sync::{watch, RwLock},
    time,
};
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

use crate::{
    agent::{
        fe_driver::FunctionExecutorFactory,
        function_executor_manager::FunctionExecutorManager,
    },
    config::AgentConfig,
    executor_api::executor_api_pb::{self, executor_api_client::ExecutorApiClient},
};

pub mod fe_driver;
mod function_executor_manager;
mod function_executor_server;
mod hardware_probe;
mod hash;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const BACKOFF_INTERVAL: Duration = Duration::from_secs(5);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
struct AgentState {
    executor_id: String,
    last_report_successful: bool,
    last_server_clock: u64,
}

pub struct Agent {
    config: AgentConfig,
    state: Arc<RwLock<AgentState>>,
    probe: Arc<hardware_probe::HardwareProbe>,
    fe_manager: Arc<RwLock<FunctionExecutorManager>>,
}

impl Agent {
    pub fn new(agent_config: AgentConfig, factory: Arc<dyn FunctionExecutorFactory>) -> Self {
        let state = AgentState {
            executor_id: nanoid::nanoid!(),
            last_report_successful: false,
            last_server_clock: 0,
        };
        Self {
            config: agent_config,
            state: Arc::new(RwLock::new(state)),
            probe: Arc::new(hardware_probe::HardwareProbe::new()),
            fe_manager: Arc::new(RwLock::new(FunctionExecutorManager::new(factory))),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!("starting agent");
        let (stream_ctrl_tx, stream_ctrl_rx) = watch::channel(false);
        let (shutdown_tx, shutdown_rx) = watch::channel(());

        // Start FE health checker loop
        {
            let fe_health_mgr = self.fe_manager.clone();
            let fe_shutdown_rx = shutdown_rx.clone();
            function_executor_manager::FunctionExecutorManager::spawn_health_loop(
                fe_health_mgr,
                fe_shutdown_rx,
            );
        }

        // Heartbeat task
        let hb_self = self.clone_for_task();
        let hb_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            hb_self.heartbeat_loop(stream_ctrl_tx, hb_shutdown_rx).await;
        });

        // Desired state stream manager
        let ds_self = self.clone_for_task();
        let ds_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            ds_self
                .desired_state_loop(stream_ctrl_rx, ds_shutdown_rx)
                .await;
        });

        info!("agent started, waiting for ctrl+c or sigterm for shutdown");

        // Signal listener
        tokio::spawn(async move {
            let ctrl_c = signal::ctrl_c();
            #[cfg(unix)]
            let mut term_sig = signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
            #[cfg(unix)]
            let terminate = term_sig.recv();
            #[cfg(not(unix))]
            let terminate = std::future::pending::<()>();

            tokio::select! {
                _ = ctrl_c => {},
                _ = terminate => {},
            }
            let _ = shutdown_tx.send(());
            info!("agent shutdown signal received");
        });

        // Wait for shutdown
        let mut main_shutdown_rx = shutdown_rx.clone();
        let _ = main_shutdown_rx.changed().await;
        Ok(())
    }

    fn clone_for_task(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: self.state.clone(),
            probe: self.probe.clone(),
            fe_manager: self.fe_manager.clone(),
        }
    }

    async fn build_channel(&self) -> anyhow::Result<Channel> {
        let addr = &self.config.grpc_server_address;
        let scheme = if self.config.tls_config.is_some() {
            "https"
        } else {
            "http"
        };
        let uri = format!("{scheme}://{addr}");
        let endpoint = Endpoint::from_shared(uri)?
            .connect_timeout(CONNECTION_TIMEOUT)
            .timeout(CONNECTION_TIMEOUT)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .http2_keep_alive_interval(CONNECTION_TIMEOUT)
            .keep_alive_while_idle(true);
        Ok(endpoint.connect().await?)
    }

    fn executor_state(
        &self,
        status: executor_api_pb::ExecutorStatus,
        server_clock: u64,
        exec_id: &str,
        resources: executor_api_pb::HostResources,
    ) -> executor_api_pb::ExecutorState {
        let fe_states =
            futures::executor::block_on(async { self.fe_manager.read().await.states() });
        let mut state = executor_api_pb::ExecutorState {
            executor_id: Some(exec_id.to_string()),
            hostname: Some(self.config.hostname.clone()),
            version: Some(self.config.executor_version.clone()),
            status: Some(status as i32),
            total_resources: Some(resources.clone()),
            total_function_executor_resources: Some(resources),
            allowed_functions: self.config.allowed_functions(),
            function_executor_states: fe_states,
            labels: self.config.labels_map(),
            state_hash: None,
            server_clock: Some(server_clock),
        };
        // Compute deterministic state hash similar to Python implementation
        let hash = Self::compute_state_hash(&state);
        state.state_hash = Some(hash);
        state
    }

    fn compute_state_hash(state: &executor_api_pb::ExecutorState) -> String {
        let mut hasher = Sha256::new();
        if let Some(ref id) = state.executor_id {
            hasher.update(id.as_bytes());
        }
        if let Some(ref host) = state.hostname {
            hasher.update(host.as_bytes());
        }
        if let Some(ref ver) = state.version {
            hasher.update(ver.as_bytes());
        }
        if let Some(st) = state.status {
            hasher.update(st.to_le_bytes());
        }
        if let Some(clock) = state.server_clock {
            hasher.update(clock.to_le_bytes());
        }
        if let Some(ref r) = state.total_resources {
            hash::HostResourcesHasher::update(&mut hasher, r);
        }
        // Labels: sort by key for determinism
        let mut labels: Vec<(&String, &String)> = state.labels.iter().collect();
        labels.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in labels {
            hasher.update(k.as_bytes());
            hasher.update(b"=");
            hasher.update(v.as_bytes());
        }
        // Allowed functions: sort by tuple (ns, app, fn, ver)
        let mut afs: Vec<(String, String, String, String)> = state
            .allowed_functions
            .iter()
            .map(|af| {
                (
                    af.namespace.clone().unwrap_or_default(),
                    af.application_name.clone().unwrap_or_default(),
                    af.function_name.clone().unwrap_or_default(),
                    af.application_version.clone().unwrap_or_default(),
                )
            })
            .collect();
        afs.sort();
        for (ns, app, f, v) in afs {
            hasher.update(ns.as_bytes());
            hasher.update(app.as_bytes());
            hasher.update(f.as_bytes());
            hasher.update(v.as_bytes());
        }
        hash::FunctionExecutorStatesHasher::update(&mut hasher, &state.function_executor_states);
        let bytes = hasher.finalize();
        hex::encode(bytes)
    }

    async fn heartbeat_loop(
        &self,
        stream_ctrl_tx: watch::Sender<bool>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        let mut status = executor_api_pb::ExecutorStatus::StartingUp;
        loop {
            if shutdown_rx.has_changed().unwrap_or(false) {
                break;
            }
            let channel = match self.build_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    error!(error=?e, "failed to build grpc channel for heartbeat");
                    self.set_report_success(false).await;
                    tokio::select! {
                        _ = time::sleep(BACKOFF_INTERVAL) => {},
                        _ = shutdown_rx.changed() => { break; }
                    }
                    continue;
                }
            };
            let mut client = ExecutorApiClient::new(channel);

            let (server_clock, exec_id) = {
                let st = self.state.read().await;
                (st.last_server_clock, st.executor_id.clone())
            };
            let resources = self.probe.total_resources();
            let req = executor_api_pb::ReportExecutorStateRequest {
                executor_state: Some(self.executor_state(
                    status,
                    server_clock,
                    &exec_id,
                    resources,
                )),
                executor_update: Some(executor_api_pb::ExecutorUpdate {
                    executor_id: Some(exec_id),
                    allocation_results: vec![],
                }),
            };

            match client.report_executor_state(req).await {
                Ok(_) => {
                    self.set_report_success(true).await;
                    let _ = stream_ctrl_tx.send_replace(true);
                    if matches!(status, executor_api_pb::ExecutorStatus::StartingUp) {
                        status = executor_api_pb::ExecutorStatus::Running;
                    }
                    debug!("heartbeat success");
                }
                Err(status_err) => {
                    warn!(error=?status_err, "heartbeat failed");
                    self.set_report_success(false).await;
                }
            }

            tokio::select! {
                _ = time::sleep(HEARTBEAT_INTERVAL) => {},
                _ = shutdown_rx.changed() => { break; }
            }
        }
    }

    async fn set_report_success(&self, success: bool) {
        let mut st = self.state.write().await;
        st.last_report_successful = success;
    }

    async fn desired_state_loop(
        &self,
        mut stream_ctrl_rx: watch::Receiver<bool>,
        mut shutdown_rx: watch::Receiver<()>,
    ) {
        loop {
            while !*stream_ctrl_rx.borrow() {
                tokio::select! {
                    res = stream_ctrl_rx.changed() => { if res.is_err() { return; } },
                    _ = shutdown_rx.changed() => { return; }
                }
            }

            let channel = match self.build_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    error!(error=?e, "failed to build grpc channel for desired state");
                    tokio::select! {
                        _ = time::sleep(BACKOFF_INTERVAL) => {},
                        _ = shutdown_rx.changed() => { return; }
                    }
                    continue;
                }
            };
            let mut client = ExecutorApiClient::new(channel);
            let exec_id = { self.state.read().await.executor_id.clone() };
            let req = executor_api_pb::GetDesiredExecutorStatesRequest {
                executor_id: Some(exec_id.clone()),
            };
            info!(executor_id=%exec_id, "opening desired state stream");
            match client.get_desired_executor_states(req).await {
                Ok(resp) => {
                    let mut stream = resp.into_inner();
                    loop {
                        if shutdown_rx.has_changed().unwrap_or(false) ||
                            !self.state.read().await.last_report_successful
                        {
                            warn!("closing desired state stream due to failed heartbeat");
                            break;
                        }
                        match stream.message().await {
                            Ok(Some(desired_state)) => {
                                let clock = desired_state.clock.unwrap_or(0);
                                {
                                    let mut st = self.state.write().await;
                                    st.last_server_clock = clock;
                                }
                                // TODO: Start/stop FEs via factory here and manage executors map
                                debug!(clock = clock, "received desired state");
                                // TODO: reconcile function executors and task
                                // allocations
                            }
                            Ok(None) => {
                                warn!("desired state stream closed by server");
                                break;
                            }
                            Err(e) => {
                                warn!(error=?e, "desired state stream error");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(error=?e, "failed to open desired state stream");
                }
            }
            tokio::select! {
                _ = time::sleep(BACKOFF_INTERVAL) => {},
                _ = shutdown_rx.changed() => { return; }
            }
        }
    }
}
