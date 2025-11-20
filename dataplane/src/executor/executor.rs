use std::{path::PathBuf, sync::Arc};

use prometheus_client::registry::Registry;

use crate::executor::{
    executor_api::executor_api_pb::ExecutorStatus,
    monitoring::{
        health_check_handler::HealthCheckHandler,
        health_checker::generic_health_checker::GenericHealthChecker,
        prometheus_metrics_handler::PrometheusMetricsHandler, server::MonitoringServer,
        startup_probe_handler::StartupProbeHandler,
    },
};

pub struct Executor {
    startup_probe_handler: StartupProbeHandler,
    channel_manager: ChannelManager,
    state_reporter: ExecutorStateReporter,
    state_reconciler: ExecutorStateReconciler,
    monitoring_server: MonitoringServer,
    registry: Arc<Registry>,
}

impl Executor {
    pub fn new(
        id: String,
        version: String,
        labels: Vec<String>,
        cache_path: PathBuf,
        // TODO: use trait here
        health_checker: GenericHealthChecker,
        function_uris: Vec<String>,
        function_executor_server_factory: FunctionExecutorServerFactory,
        server_addr: String,
        grpc_server_addr: String,
        config_path: Option<String>,
        monitoring_server_host: String,
        monitoring_server_port: u16,
        blob_store: BlobStore,
        host_resource_provider: HostResourceProvider,
        catalog_entry_name: Option<String>,
        registry: Arc<Registry>,
    ) -> Self {
        let startup_probe_handler = StartupProbeHandler::new();
        let state_reporter = ExecutorStateReporter::new(
            id.clone(),
            version.clone(),
            labels.clone(),
            function_uris.clone(),
            channel_manager.clone(),
            host_resource_provider.clone(),
            health_checker.clone(),
            catalog_entry_name.clone(),
        );
        let state_reconciler = ExecutorStateReconciler::new(
            id,
            function_executor_server_factory,
            server_addr,
            config_path,
            cache_path,
            blob_store,
            channel_manager,
            state_reporter,
        );
        let channel_manager = ChannelManager::new(grpc_server_addr, config_path.clone());
        state_reporter.update_executor_status(ExecutorStatus::StartingUp);
        Executor {
            startup_probe_handler,
            channel_manager,
            state_reporter,
            state_reconciler,
            monitoring_server: MonitoringServer::new(
                monitoring_server_host,
                monitoring_server_port,
                startup_probe_handler,
                HealthCheckHandler::new(health_checker),
                PrometheusMetricsHandler::new(registry.clone()),
                ReportedStateHandler::new(state_reporter),
                DesiredStateHandler::new(state_reconciler),
            ),
            registry,
        }
    }

    pub fn run(&self) {}
}
