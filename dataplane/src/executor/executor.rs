use std::{collections::HashMap, path::PathBuf, sync::Arc};

use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use crate::executor::{
    blob_store::BlobStore,
    executor_api::{
        executor_api_pb::ExecutorStatus, ChannelManager, ExecutorStateReconciler,
        ExecutorStateReporter, FunctionUri,
    },
    function_executor::server_factory::SubprocessFunctionExecutorServerFactory,
    host_resources::HostResourcesProvider,
    monitoring::{
        desired_state_handler::DesiredStateHandler, health_check_handler::HealthCheckHandler,
        health_checker::generic_health_checker::GenericHealthChecker,
        prometheus_metrics_handler::PrometheusMetricsHandler,
        reported_state_handler::ReportedStateHandler, server::MonitoringServer,
        startup_probe_handler::StartupProbeHandler,
    },
};

pub struct Executor {
    startup_probe_handler: StartupProbeHandler,
    channel_manager: Arc<ChannelManager>,
    state_reporter: ExecutorStateReporter,
    state_reconciler: Arc<Mutex<ExecutorStateReconciler>>,
    monitoring_server: MonitoringServer,
    registry: Arc<Registry>,
}

impl Executor {
    pub async fn new(
        id: String,
        version: String,
        labels: &mut HashMap<String, String>,
        cache_path: PathBuf,
        // TODO: use trait here
        health_checker: Arc<Mutex<GenericHealthChecker>>,
        function_uris: Vec<String>,
        // TODO: use trait here
        function_executor_server_factory: SubprocessFunctionExecutorServerFactory,
        grpc_server_addr: String,
        config_path: Option<String>,
        monitoring_server_host: String,
        monitoring_server_port: u16,
        blob_store: BlobStore,
        host_resource_provider: HostResourcesProvider,
        catalog_entry_name: Option<String>,
        registry: Arc<Registry>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let channel_manager =
            Arc::new(ChannelManager::new(grpc_server_addr, config_path.clone()).await?);
        let startup_probe_handler = StartupProbeHandler::new();
        let mut state_reporter = ExecutorStateReporter::new(
            id.clone(),
            version.clone(),
            labels,
            parse_function_uris(function_uris),
            channel_manager.clone(),
            host_resource_provider,
            health_checker.clone(),
            catalog_entry_name.clone(),
        );
        let state_reconciler = Arc::new(Mutex::new(ExecutorStateReconciler::new(
            id,
            function_executor_server_factory,
            cache_path,
            blob_store,
            channel_manager.clone(),
            state_reporter.clone(),
        )));
        state_reporter.update_executor_status(ExecutorStatus::StartingUp);
        Ok(Executor {
            startup_probe_handler,
            channel_manager,
            state_reporter: state_reporter.clone(),
            state_reconciler: state_reconciler.clone(),
            monitoring_server: MonitoringServer::new(
                monitoring_server_host,
                monitoring_server_port,
                startup_probe_handler,
                HealthCheckHandler::new(health_checker.clone()),
                PrometheusMetricsHandler::new(registry.clone()),
                ReportedStateHandler::new(state_reporter),
                DesiredStateHandler::new(state_reconciler),
            ),
            registry,
        })
    }

    pub fn run(&self) {}
}

fn parse_function_uris(vec: Vec<String>) -> Vec<FunctionUri> {
    vec.into_iter()
        .map(|uri| {
            FunctionUri::new(
                uri.split(':').nth(0).unwrap_or_default().to_string(),
                uri.split(':').nth(1).unwrap_or_default().to_string(),
                uri.split(':').nth(2).unwrap_or_default().to_string(),
                uri.split(':').nth(3).map(|v| v.to_string()),
            )
        })
        .collect()
}
