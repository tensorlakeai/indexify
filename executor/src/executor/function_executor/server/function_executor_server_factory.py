from typing import Any, Optional

from .function_executor_server import FunctionExecutorServer


class FunctionExecutorServerConfiguration:
    """Configuration for creating a FunctionExecutorServer.

    This configuration only includes data that must be known
    during creation of the FunctionExecutorServer. If some data
    is not required during the creation then it shouldn't be here.

    A particular factory implementation might ignore certain
    configuration parameters or raise an exception if it can't implement
    them."""

    def __init__(self, image_uri: Optional[str]):
        # Container image URI of the Function Executor Server.
        self.image_uri: Optional[str] = image_uri


class FunctionExecutorServerFactory:
    """Abstract class for creating FunctionExecutorServers."""

    async def create(
        self, config: FunctionExecutorServerConfiguration, logger: Any
    ) -> FunctionExecutorServer:
        """Creates a new FunctionExecutorServer.

        Raises an exception if the creation failed or the configuration is not supported.
        Args:
            config: configuration of the FunctionExecutorServer.
            logger: logger to be used during the function call."""
        raise NotImplementedError()

    async def destroy(self, server: FunctionExecutorServer, logger: Any) -> None:
        """Destroys the FunctionExecutorServer and release all its resources.

        Args:
            logger: logger to be used during the function call.
        FunctionExecutorServer and customer code that it's running are not notified about the destruction.
        Never raises any Exceptions."""
        raise NotImplementedError
