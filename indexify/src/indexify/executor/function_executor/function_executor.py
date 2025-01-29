import asyncio
from typing import Any, Optional

import grpc
from tensorlake.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
)
from tensorlake.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)
from tensorlake.utils.http_client import get_httpx_client

from .health_checker import HealthChecker
from .invocation_state_client import InvocationStateClient
from .server.function_executor_server import (
    FUNCTION_EXECUTOR_SERVER_READY_TIMEOUT_SEC,
    FunctionExecutorServer,
)
from .server.function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)


class CustomerError(RuntimeError):
    pass


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
        self._initialized = False

    async def initialize(
        self,
        config: FunctionExecutorServerConfiguration,
        initialize_request: InitializeRequest,
        base_url: str,
        config_path: Optional[str],
    ):
        """Creates and initializes a FunctionExecutorServer and all resources associated with it.

        Raises CustomerError if the server failed to initialize due to an error in customer owned code or data.
        Raises an Exception if an internal error occured."""
        try:
            self._server = await self._server_factory.create(
                config=config, logger=self._logger
            )
            self._channel = await self._server.create_channel(self._logger)
            await _channel_ready(self._channel)

            stub: FunctionExecutorStub = FunctionExecutorStub(self._channel)
            await _initialize_server(stub, initialize_request)

            self._invocation_state_client = InvocationStateClient(
                stub=stub,
                base_url=base_url,
                http_client=get_httpx_client(config_path=config_path, make_async=True),
                graph=initialize_request.graph_name,
                namespace=initialize_request.namespace,
                logger=self._logger,
            )
            await self._invocation_state_client.start()

            self._health_checker = HealthChecker(
                stub=stub,
                logger=self._logger,
            )

            self._initialized = True
        except Exception:
            await self.destroy()
            raise

    def channel(self) -> grpc.aio.Channel:
        self._check_initialized()
        return self._channel

    def invocation_state_client(self) -> InvocationStateClient:
        self._check_initialized()
        return self._invocation_state_client

    def health_checker(self) -> HealthChecker:
        self._check_initialized()
        return self._health_checker

    async def destroy(self):
        """Destroys all resources owned by this FunctionExecutor.

        Never raises any exceptions but logs them."""
        try:
            if self._health_checker is not None:
                self._health_checker.stop()
                self._health_checker = None
        except Exception as e:
            self._logger.error("failed to stop HealthChecker", exc_info=e)

        try:
            if self._invocation_state_client is not None:
                await self._invocation_state_client.destroy()
                self._invocation_state_client = None
        except Exception as e:
            self._logger.error(
                "failed to destroy FunctionExecutor invocation state client", exc_info=e
            )

        try:
            if self._channel is not None:
                await self._channel.close()
                self._channel = None
        except Exception as e:
            self._logger.error(
                "failed to close FunctionExecutorServer channel", exc_info=e
            )

        try:
            if self._server is not None:
                await self._server_factory.destroy(self._server, self._logger)
                self._server = None
        except Exception as e:
            self._logger.error("failed to destroy FunctionExecutorServer", exc_info=e)

    def _check_initialized(self):
        if not self._initialized:
            raise RuntimeError("FunctionExecutor is not initialized")


async def _channel_ready(channel: grpc.aio.Channel):
    await asyncio.wait_for(
        channel.channel_ready(),
        timeout=FUNCTION_EXECUTOR_SERVER_READY_TIMEOUT_SEC,
    )


async def _initialize_server(
    stub: FunctionExecutorStub, initialize_request: InitializeRequest
):
    initialize_response: InitializeResponse = await stub.initialize(initialize_request)
    if initialize_response.success:
        return
    if initialize_response.HasField("customer_error"):
        raise CustomerError(initialize_response.customer_error)
    else:
        raise Exception("initialize RPC failed at function executor server")
