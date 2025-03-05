import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Optional

from grpc.aio import AioRpcError
from tensorlake.function_executor.proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from .metrics.health_checker import (
    metric_failed_health_checks,
    metric_health_check_latency,
)
from .server.client_configuration import HEALTH_CHECK_TIMEOUT_SEC

HEALTH_CHECK_POLL_PERIOD_SEC = 10


class HealthCheckResult:
    def __init__(self, is_healthy: bool, reason: str):
        self.is_healthy: bool = is_healthy
        self.reason: str = reason


class HealthChecker:
    def __init__(self, stub: FunctionExecutorStub, logger: Any):
        self._stub: FunctionExecutorStub = stub
        self._logger: Any = logger.bind(module=__name__)
        self._health_check_loop_task: Optional[asyncio.Task] = None
        self._health_check_failed_callback: Optional[
            Callable[[HealthCheckResult], Awaitable[None]]
        ] = None

    async def check(self) -> HealthCheckResult:
        """Runs the health check once and returns the result.

        Does not raise any exceptions."""
        with metric_health_check_latency.time():
            try:
                response: HealthCheckResponse = await self._stub.check_health(
                    HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                )
                if not response.healthy:
                    metric_failed_health_checks.inc()
                return HealthCheckResult(
                    is_healthy=response.healthy, reason=response.status_message
                )
            except AioRpcError as e:
                metric_failed_health_checks.inc()
                # Expected exception when there are problems with communication because e.g. the server is unhealthy.
                return HealthCheckResult(
                    is_healthy=False,
                    reason=f"Executor side RPC channel error: {str(e)}",
                )
            except Exception as e:
                metric_failed_health_checks.inc()
                self._logger.warning("Got unexpected exception, ignoring", exc_info=e)
                return HealthCheckResult(
                    is_healthy=False,
                    reason=f"Unexpected exception in Executor: {str(e)}",
                )

    def start(self, callback: Callable[[HealthCheckResult], Awaitable[None]]) -> None:
        """Starts periodic health checks.

        The supplied callback is an async function called in the calling thread's
        event loop when the health check fails. The callback is called only once
        and then health checks are stopped.

        Without a periodic health check a TCP client socket won't detect a server
        socket problem (e.g. it's closed due to server crash) because there are no
        TCP packets floating between them.

        Does not raise any exceptions.
        """
        if self._health_check_loop_task is not None:
            return

        self._health_check_failed_callback = callback
        self._health_check_loop_task = asyncio.create_task(self._health_check_loop())

    def stop(self) -> None:
        """Stops the periodic health checks.

        Does not raise any exceptions."""
        if self._health_check_loop_task is None:
            return

        self._health_check_loop_task.cancel()
        self._health_check_loop_task = None

    async def _health_check_loop(self) -> None:
        while True:
            result: HealthCheckResult = await self.check()
            if not result.is_healthy:
                break
            await asyncio.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)

        asyncio.create_task(self._health_check_failed_callback(result))
        self._health_check_loop_task = None
