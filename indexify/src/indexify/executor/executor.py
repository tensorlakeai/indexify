import asyncio
import signal
from pathlib import Path
from socket import gethostname
from typing import Dict, List, Optional

import structlog

from indexify.proto.executor_api_pb2 import ExecutorStatus

from .blob_store.blob_store import BLOBStore
from .channel_manager import ChannelManager
from .function_allowlist import (
    FunctionURI,
    function_allowlist_to_indexed_dict,
    parse_function_uris,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .host_resources.host_resources import HostResourcesProvider
from .metrics.executor import (
    metric_executor_info,
    metric_executor_state,
)
from .monitoring.health_check_handler import HealthCheckHandler
from .monitoring.health_checker.health_checker import HealthChecker
from .monitoring.prometheus_metrics_handler import PrometheusMetricsHandler
from .monitoring.server import MonitoringServer
from .monitoring.startup_probe_handler import StartupProbeHandler
from .state_reconciler import ExecutorStateReconciler
from .state_reporter import ExecutorStateReporter

metric_executor_state.state("starting")


class Executor:
    def __init__(
        self,
        id: str,
        version: str,
        labels: Dict[str, str],
        cache_path: Path,
        health_checker: HealthChecker,
        function_uris: List[str],
        function_executor_server_factory: FunctionExecutorServerFactory,
        server_addr: str,
        grpc_server_addr: str,
        config_path: Optional[str],
        monitoring_server_host: str,
        monitoring_server_port: int,
        blob_store: BLOBStore,
        host_resources_provider: HostResourcesProvider,
    ):
        self._logger = structlog.get_logger(module=__name__, executor_id=id)
        protocol: str = "http"
        if config_path:
            self._logger.info("running the extractor with TLS enabled")
            protocol = "https"

        self._startup_probe_handler = StartupProbeHandler()
        self._monitoring_server = MonitoringServer(
            host=monitoring_server_host,
            port=monitoring_server_port,
            startup_probe_handler=self._startup_probe_handler,
            health_probe_handler=HealthCheckHandler(health_checker),
            metrics_handler=PrometheusMetricsHandler(),
        )
        self._channel_manager = ChannelManager(
            server_address=grpc_server_addr,
            config_path=config_path,
            logger=self._logger,
        )
        function_allowlist: List[FunctionURI] = parse_function_uris(function_uris)
        self._state_reporter = ExecutorStateReporter(
            executor_id=id,
            version=version,
            labels=labels,
            function_allowlist=function_allowlist,
            channel_manager=self._channel_manager,
            host_resources_provider=host_resources_provider,
            health_checker=health_checker,
            logger=self._logger,
        )
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_STARTING_UP
        )
        self._state_reconciler = ExecutorStateReconciler(
            executor_id=id,
            function_executor_server_factory=function_executor_server_factory,
            base_url=f"{protocol}://{server_addr}",
            config_path=config_path,
            cache_path=cache_path,
            blob_store=blob_store,
            channel_manager=self._channel_manager,
            state_reporter=self._state_reporter,
            logger=self._logger,
        )
        self._run_aio_task: Optional[asyncio.Task] = None
        self._shutdown_aio_task: Optional[asyncio.Task] = None

        executor_info: Dict[str, str] = {
            "id": id,
            "version": version,
            "cache_path": str(cache_path),
            "server_addr": server_addr,
            "grpc_server_addr": str(grpc_server_addr),
            "config_path": str(config_path),
            "hostname": gethostname(),
        }
        for key, value in labels.items():
            executor_info["label_" + key] = value
        executor_info.update(function_allowlist_to_indexed_dict(function_allowlist))
        metric_executor_info.info(executor_info)

    def run(self):
        asyncio.new_event_loop()

        self._run_aio_task = asyncio.get_event_loop().create_task(
            self._run(),
            name="executor startup and run loop",
        )

        try:
            asyncio.get_event_loop().run_until_complete(self._run_aio_task)
        except asyncio.CancelledError:
            pass  # Expected exception on shutdown

    async def _run(self):
        for signum in [
            signal.SIGABRT,
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGQUIT,
            signal.SIGHUP,
        ]:
            asyncio.get_event_loop().add_signal_handler(
                signum, self._shutdown_signal_handler, asyncio.get_event_loop()
            )

        asyncio.create_task(
            self._monitoring_server.run(), name="monitoring server runner"
        )
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_RUNNING
        )
        self._state_reporter.run()
        self._state_reconciler.run()
        metric_executor_state.state("running")
        self._startup_probe_handler.set_ready()

        # Run the Executor forever until it is shut down.
        while True:
            await asyncio.sleep(10)

    def _shutdown_signal_handler(self, loop):
        if self._shutdown_aio_task is None:
            self._shutdown_aio_task = loop.create_task(
                self._shutdown(), name="executor shutdown"
            )

    async def _shutdown(self):
        self._logger.info("shutting down Executor")
        metric_executor_state.state("shutting_down")

        # Shutdown state reconciler first because it changes reported state on shutdown.
        await self._state_reconciler.shutdown()

        # Do one last state report with STOPPED status. This reduces latency in the system.
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_STOPPED
        )
        await self._state_reporter.shutdown()
        await self._channel_manager.destroy()
        await self._monitoring_server.shutdown()
        self._run_aio_task.cancel()
