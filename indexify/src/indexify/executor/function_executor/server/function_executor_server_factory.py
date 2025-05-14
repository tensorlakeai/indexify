from dataclasses import dataclass
from typing import Any, List, Optional

from .function_executor_server import FunctionExecutorServer


@dataclass
class FunctionExecutorServerConfiguration:
    """Configuration for creating a FunctionExecutorServer.

    This configuration only includes data that must be known
    during creation of the FunctionExecutorServer. If some data
    is not required during the creation then it shouldn't be here.

    A particular factory implementation might ignore certain
    configuration parameters or raise an exception if it can't implement
    them."""

    executor_id: str
    function_executor_id: str
    namespace: str
    graph_name: str
    function_name: str
    graph_version: str
    image_uri: Optional[str]
    secret_names: List[str]
    cpu_ms_per_sec: int
    memory_bytes: int
    disk_bytes: int
    gpu_count: int


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
