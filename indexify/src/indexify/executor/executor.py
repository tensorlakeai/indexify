import asyncio
import signal
from pathlib import Path
from socket import gethostname
from typing import Dict, List, Optional

import structlog
from tensorlake.utils.logging import suppress as suppress_logging

from indexify.proto.executor_api_pb2 import ExecutorStatus

from .blob_store.blob_store import BLOBStore
from .downloader import Downloader
from .function_allowlist import (
    FunctionURI,
    function_allowlist_to_indexed_dict,
    parse_function_uris,
)
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .grpc.channel_manager import ChannelManager
from .grpc.state_reconciler import ExecutorStateReconciler
from .grpc.state_reporter import ExecutorStateReporter
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
from .task_reporter import TaskReporter

metric_executor_state.state("starting")


class Executor:
    def __init__(
        self,
        id: str,
        version: str,
        labels: Dict[str, str],
        code_path: Path,
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
        self._logger = structlog.get_logger(module=__name__)
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
        self._function_executor_states = FunctionExecutorStatesContainer(
            logger=self._logger
        )
        health_checker.set_function_executor_states_container(
            self._function_executor_states
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
            function_executor_states=self._function_executor_states,
            channel_manager=self._channel_manager,
            host_resources_provider=host_resources_provider,
            logger=self._logger,
        )
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_STARTING_UP
        )
        self._task_reporter = TaskReporter(
            executor_id=id,
            channel_manager=self._channel_manager,
            blob_store=blob_store,
        )
        self._state_reconciler = ExecutorStateReconciler(
            executor_id=id,
            function_executor_server_factory=function_executor_server_factory,
            base_url=f"{protocol}://{server_addr}",
            function_executor_states=self._function_executor_states,
            config_path=config_path,
            downloader=Downloader(
                code_path=code_path,
                blob_store=blob_store,
            ),
            task_reporter=self._task_reporter,
            channel_manager=self._channel_manager,
            state_reporter=self._state_reporter,
            logger=self._logger,
        )

        executor_info: Dict[str, str] = {
            "id": id,
            "version": version,
            "code_path": str(code_path),
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
        for signum in [
            signal.SIGABRT,
            signal.SIGINT,
            signal.SIGTERM,
            signal.SIGQUIT,
            signal.SIGHUP,
        ]:
            asyncio.get_event_loop().add_signal_handler(
                signum, self.shutdown, asyncio.get_event_loop()
            )

        asyncio.get_event_loop().create_task(
            self._monitoring_server.run(), name="monitoring server runner"
        )
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_RUNNING
        )
        asyncio.get_event_loop().create_task(
            self._state_reporter.run(), name="state reporter runner"
        )

        metric_executor_state.state("running")
        self._startup_probe_handler.set_ready()

        try:
            asyncio.get_event_loop().run_until_complete(self._state_reconciler.run())
        except asyncio.CancelledError:
            pass  # Suppress this expected exception and return without error (normally).

    async def _shutdown(self, loop):
        self._logger.info(
            "shutting down, all Executor logs are suppressed, no task outcomes will be reported to Server from this point"
        )
        self._state_reporter.update_executor_status(
            ExecutorStatus.EXECUTOR_STATUS_STOPPING
        )
        metric_executor_state.state("shutting_down")
        # There will be lots of task cancellation exceptions and "X is shutting down"
        # exceptions logged during Executor shutdown. Suppress their logs as they are
        # expected and are confusing for users.
        suppress_logging()

        await self._monitoring_server.shutdown()
        await self._task_reporter.shutdown()
        await self._state_reporter.shutdown()
        await self._state_reconciler.shutdown()
        await self._channel_manager.destroy()

        # We need to shutdown all users of FE states first,
        # otherwise states might disappear unexpectedly and we might
        # report errors, etc that are expected.
        await self._function_executor_states.shutdown()
        # We mainly need to cancel the task that runs _.*_mode_loop().
        for task in asyncio.all_tasks(loop):
            task.cancel()
        # The current task is cancelled, the code after this line will not run.

    def shutdown(self, loop):
        loop.create_task(self._shutdown(loop), name="executor shutdown")
