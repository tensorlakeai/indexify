use std::path::PathBuf;

pub struct Executor {
    startup_probe_handler: StartupProbeHandler,
    channel_manager: ChannelManager,
    state_reporter: ExecutorStateReporter,
    state_reconciler: ExecutorStateReconciler,
    monitoring_server: MonitoringServer,
}

impl Executor {
    pub fn new(
        id: String,
        version: String,
        labels: Vec<String>,
        cache_path: PathBuf,
        health_checker: HealthChecker,
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
    ) -> Self {
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
        state_reporter.update_executor_status(ExecutorStatus::EXECUTOR_STATUS_STARTING_UP);
        Executor {
            startup_probe_handler: StartupProbeHandler::new(),
            channel_manager: ChannelManager::new(grpc_server_addr, config_path.clone()),
            state_reporter,
            state_reconciler: ExecutorStateReconciler::new(
                id,
                function_executor_server_factory,
                server_addr,
                config_path,
                cache_path,
                blob_store,
                self.channel_manager.clone(),
                self.state_reporter.clone(),
            ),
            monitoring_server: MonitoringServer::new(
                monitoring_server_host,
                monitoring_server_port,
                self.startup_probe_handler,
                HealthCheckHandler::new(health_checker),
                PrometheusMetricsHandler::new(),
                ReportedStateHandler::new(self.state_reporter.clone()),
                DesiredStateHandler::new(self.state_reconciler.clone()),
            ),
        }
    }

    pub fn run(&self) {}
}
