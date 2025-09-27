import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

import grpc
import grpc.aio
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

# Use lowest feasible value for now to detect FE crashes quickly because
# we're only doing periodic health checks now.
HEALTH_CHECK_POLL_PERIOD_SEC = 5


class HealthCheckResult:
    def __init__(self, is_healthy: bool, reason: str):
        self.is_healthy: bool = is_healthy
        self.reason: str = reason


class HealthChecker:
    def __init__(
        self, channel: grpc.aio.Channel, stub: FunctionExecutorStub, logger: Any
    ):
        self._channel: grpc.aio.Channel = channel
        self._stub: FunctionExecutorStub = stub
        self._logger: Any = logger.bind(module=__name__)
        self._health_check_loop_task: asyncio.Task | None = None
        self._health_check_failed_callback: Callable[
            [HealthCheckResult], Awaitable[None] | None
        ] = None

    async def check(self) -> HealthCheckResult:
        """Runs the health check once and returns the result.

        Does not raise any exceptions."""
        if os.getenv("INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS", "0") == "1":
            return HealthCheckResult(
                is_healthy=True,
                reason="Function Executor health checks are disabled using INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS env var.",
            )

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
            except grpc.aio.AioRpcError as e:
                # Due to the customer code running in Function Executor we can't reliably conclude
                # that the FE is unhealthy when RPC status code is not OK. E.g. customer code can
                # hold Python GIL and prevent the health check RPC from being processed by FE Python code.
                #
                # The only unhealthy condition we can be sure about is when the channel can't re-establish
                # the TCP connection within HEALTH_CHECK_TIMEOUT_SEC deadline. This is because FE Python
                # code is not involved when TCP connections are established to FE. Problems reestablishing
                # the TCP connection are usually due to the FE process crashing and its gRPC server socket
                # not being available anymore or due to prolonged local networking failures on Executor.
                if (
                    _channel_state(self._channel, self._logger)
                    == grpc.ChannelConnectivity.TRANSIENT_FAILURE
                ):
                    return HealthCheckResult(
                        is_healthy=False,
                        reason="Channel is in TRANSIENT_FAILURE state, assuming Function Executor crashed.",
                    )
                else:
                    return HealthCheckResult(
                        is_healthy=True,
                        reason=f"Health check RPC failed with status code: {e.code().name}. Assuming Function Executor is healthy.",
                    )
            except Exception as e:
                self._logger.error("Got unexpected exception, ignoring", exc_info=e)
                return HealthCheckResult(
                    is_healthy=True,
                    reason=f"Unexpected exception in Executor: {str(e)}. Assuming Function Executor is healthy.",
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
        self._health_check_loop_task = asyncio.create_task(
            self._health_check_loop(), name="function executor health checker loop"
        )

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

        asyncio.create_task(
            self._health_check_failed_callback(result),
            name="function executor health check failure callback",
        )
        self._health_check_loop_task = None


def _channel_state(channel: grpc.aio.Channel, logger: Any) -> grpc.ChannelConnectivity:
    """Get channel connectivity state and suppresses all exceptions.

    Suppressing the exceptions is important because the channel connectivity state is an experimental
    feature. On error fallse back to READY state which assumes that the channel is okay.
    """
    try:
        return channel.get_state()
    except Exception as e:
        logger.error(
            "Failed getting channel state, falling back to default READY state",
            exc_info=e,
        )
        return grpc.ChannelConnectivity.READY
