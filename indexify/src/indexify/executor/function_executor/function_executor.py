import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InfoRequest,
    InfoResponse,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.function_executor.proto.message_validator import MessageValidator
from tensorlake.utils.http_client import get_httpx_client

from .health_checker import HealthChecker
from .invocation_state_client import InvocationStateClient
from .metrics.function_executor import (
    metric_create_errors,
    metric_create_health_checker_errors,
    metric_create_health_checker_latency,
    metric_create_invocation_state_client_errors,
    metric_create_invocation_state_client_latency,
    metric_create_latency,
    metric_create_server_errors,
    metric_create_server_latency,
    metric_creations,
    metric_destroy_channel_errors,
    metric_destroy_channel_latency,
    metric_destroy_errors,
    metric_destroy_health_checker_errors,
    metric_destroy_health_checker_latency,
    metric_destroy_invocation_state_client_errors,
    metric_destroy_invocation_state_client_latency,
    metric_destroy_latency,
    metric_destroy_server_errors,
    metric_destroy_server_latency,
    metric_destroys,
    metric_establish_channel_errors,
    metric_establish_channel_latency,
    metric_function_executor_infos,
    metric_function_executors_count,
    metric_get_info_rpc_errors,
    metric_get_info_rpc_latency,
    metric_initialize_rpc_errors,
    metric_initialize_rpc_latency,
)
from .server.function_executor_server import (
    FUNCTION_EXECUTOR_SERVER_READY_TIMEOUT_SEC,
    FunctionExecutorServer,
)
from .server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)


class FunctionExecutorInitializationError(Enum):
    FUNCTION_TIMEOUT = 1
    FUNCTION_ERROR = 2


@dataclass
class FunctionExecutorInitializationResult:
    """Result of FunctionExecutor initialization."""

    # None error means success.
    error: Optional[FunctionExecutorInitializationError] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None


class FunctionExecutor:
    """Executor side class supporting a running FunctionExecutorServer.

    FunctionExecutor primary responsibility is creation and initialization
    of all resources associated with a particular Function Executor Server
    including the Server itself. FunctionExecutor owns all these resources
    and provides other Executor components with access to them.

    Addition of any business logic besides resource management is discouraged.
    Please add such logic to other classes managed by this class.
    """

    def __init__(self, server_factory: FunctionExecutorServerFactory, logger: Any):
        self._server_factory: FunctionExecutorServerFactory = server_factory
        self._logger = logger.bind(module=__name__)
        self._server: Optional[FunctionExecutorServer] = None
        self._channel: Optional[grpc.aio.Channel] = None
        self._invocation_state_client: Optional[InvocationStateClient] = None
        self._health_checker: Optional[HealthChecker] = None
        metric_function_executors_count.inc()

    async def initialize(
        self,
        config: FunctionExecutorServerConfiguration,
        initialize_request: InitializeRequest,
        base_url: str,
        config_path: Optional[str],
        customer_code_timeout_sec: Optional[float] = None,
    ) -> FunctionExecutorInitializationResult:
        """Creates and initializes a FunctionExecutorServer and all resources associated with it.

        Raises an Exception if an internal error occured."""
        try:
            with (
                metric_create_errors.count_exceptions(),
                metric_create_latency.time(),
            ):
                metric_creations.inc()
                await self._create_server(config)
                await self._establish_channel()
                stub: FunctionExecutorStub = FunctionExecutorStub(self._channel)
                await _collect_server_info(stub)
                await self._create_invocation_state_client(
                    stub=stub,
                    base_url=base_url,
                    config_path=config_path,
                    initialize_request=initialize_request,
                )
                await self._create_health_checker(self._channel, stub)

                return await _initialize_server(
                    stub, initialize_request, customer_code_timeout_sec
                )
        except Exception:
            await self.destroy()
            raise

    def channel(self) -> grpc.aio.Channel:
        return self._channel

    def invocation_state_client(self) -> InvocationStateClient:
        return self._invocation_state_client

    def health_checker(self) -> HealthChecker:
        return self._health_checker

    async def destroy(self):
        """Destroys all resources owned by this FunctionExecutor.

        Never raises any exceptions but logs them.
        Idempotent.
        """
        try:
            with (
                metric_destroy_errors.count_exceptions(),
                metric_destroy_latency.time(),
            ):
                metric_function_executors_count.dec()
                metric_destroys.inc()
                await self._destroy_health_checker()
                await self._destroy_invocation_state_client()
                await self._destroy_channel()
                await self._destroy_server()
        except Exception as e:
            self._logger.error(
                "exception from a Function Executor destroy step, some destroy steps are not executed, this is a resource leak",
                exc_info=e,
            )

    async def _create_server(self, config: FunctionExecutorServerConfiguration) -> None:
        with (
            metric_create_server_errors.count_exceptions(),
            metric_create_server_latency.time(),
        ):
            self._server = await self._server_factory.create(
                config=config, logger=self._logger
            )

    async def _destroy_server(self) -> None:
        if self._server is None:
            return

        try:
            with (
                metric_destroy_server_errors.count_exceptions(),
                metric_destroy_server_latency.time(),
            ):
                await self._server_factory.destroy(self._server, self._logger)
        except Exception as e:
            self._logger.error("failed to destroy FunctionExecutorServer", exc_info=e)
        finally:
            self._server = None

    async def _establish_channel(self) -> None:
        with (
            metric_establish_channel_errors.count_exceptions(),
            metric_establish_channel_latency.time(),
        ):
            self._channel = await self._server.create_channel(self._logger)
            await asyncio.wait_for(
                self._channel.channel_ready(),
                timeout=FUNCTION_EXECUTOR_SERVER_READY_TIMEOUT_SEC,
            )

    async def _destroy_channel(self) -> None:
        if self._channel is None:
            return

        try:
            with (
                metric_destroy_channel_errors.count_exceptions(),
                metric_destroy_channel_latency.time(),
            ):
                await self._channel.close()
        except Exception as e:
            self._logger.error(
                "failed to close FunctionExecutorServer channel", exc_info=e
            )
        finally:
            self._channel = None

    async def _create_invocation_state_client(
        self,
        stub: FunctionExecutorStub,
        base_url: str,
        config_path: Optional[str],
        initialize_request: InitializeRequest,
    ) -> None:
        with (
            metric_create_invocation_state_client_errors.count_exceptions(),
            metric_create_invocation_state_client_latency.time(),
        ):
            self._invocation_state_client = InvocationStateClient(
                stub=stub,
                base_url=base_url,
                http_client=get_httpx_client(config_path=config_path, make_async=True),
                graph=initialize_request.graph_name,
                namespace=initialize_request.namespace,
                logger=self._logger,
            )
            await self._invocation_state_client.start()

    async def _destroy_invocation_state_client(self) -> None:
        if self._invocation_state_client is None:
            return

        try:
            with (
                metric_destroy_invocation_state_client_errors.count_exceptions(),
                metric_destroy_invocation_state_client_latency.time(),
            ):
                await self._invocation_state_client.destroy()
        except Exception as e:
            self._logger.error(
                "failed to destroy FunctionExecutor invocation state client", exc_info=e
            )
        finally:
            self._invocation_state_client = None

    async def _create_health_checker(
        self, channel: grpc.aio.Channel, stub: FunctionExecutorStub
    ) -> None:
        with (
            metric_create_health_checker_errors.count_exceptions(),
            metric_create_health_checker_latency.time(),
        ):
            self._health_checker = HealthChecker(
                channel=channel,
                stub=stub,
                logger=self._logger,
            )

    async def _destroy_health_checker(self) -> None:
        if self._health_checker is None:
            return

        try:
            with (
                metric_destroy_health_checker_errors.count_exceptions(),
                metric_destroy_health_checker_latency.time(),
            ):
                self._health_checker.stop()
        except Exception as e:
            self._logger.error("failed to stop HealthChecker", exc_info=e)
        finally:
            self._health_checker = None


async def _collect_server_info(stub: FunctionExecutorStub) -> None:
    with (
        metric_get_info_rpc_errors.count_exceptions(),
        metric_get_info_rpc_latency.time(),
    ):
        info: InfoResponse = await stub.get_info(InfoRequest())
        validator = MessageValidator(info)
        validator.required_field("version")
        validator.required_field("sdk_version")
        validator.required_field("sdk_language")
        validator.required_field("sdk_language_version")

        metric_function_executor_infos.labels(
            version=info.version,
            sdk_version=info.sdk_version,
            sdk_language=info.sdk_language,
            sdk_language_version=info.sdk_language_version,
        ).inc()


async def _initialize_server(
    stub: FunctionExecutorStub,
    initialize_request: InitializeRequest,
    customer_code_timeout_sec: Optional[float],
) -> FunctionExecutorInitializationResult:
    with (
        metric_initialize_rpc_errors.count_exceptions(),
        metric_initialize_rpc_latency.time(),
    ):
        try:
            initialize_response: InitializeResponse = await stub.initialize(
                initialize_request,
                timeout=customer_code_timeout_sec,
            )

            if (
                initialize_response.outcome_code
                == InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS
            ):
                return FunctionExecutorInitializationResult(
                    stdout=initialize_response.stdout, stderr=initialize_response.stderr
                )
            elif (
                initialize_response.outcome_code
                == InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_FAILURE
            ):
                if (
                    initialize_response.failure_reason
                    == InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR
                ):
                    return FunctionExecutorInitializationResult(
                        error=FunctionExecutorInitializationError.FUNCTION_ERROR,
                        stdout=initialize_response.stdout,
                        stderr=initialize_response.stderr,
                    )
                elif (
                    initialize_response.failure_reason
                    == InitializationFailureReason.INITIALIZATION_FAILURE_REASON_INTERNAL_ERROR
                ):
                    # Don't add stdout/stderr because this is customer data.
                    raise RuntimeError("initialize RPC failed with internal error")
                else:
                    raise ValueError(
                        f"unexpected failure reason {InitializationFailureReason.Name(initialize_response.failure_reason)} in initialize RPC response"
                    )
            else:
                raise ValueError(
                    f"unexpected outcome code {InitializationOutcomeCode.Name(initialize_response.outcome_code)} in initialize RPC response"
                )

        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                return FunctionExecutorInitializationResult(
                    error=FunctionExecutorInitializationError.FUNCTION_TIMEOUT,
                    stderr=f"Function initialization exceeded its configured timeout of {customer_code_timeout_sec:.3f} sec.",
                )
            raise
