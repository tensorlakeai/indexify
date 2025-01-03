from typing import Any

import grpc

# Timeout for Function Executor Server startup in seconds. The timeout is counted from
# the moment when a server just started.
FUNCTION_EXECUTOR_SERVER_READY_TIMEOUT_SEC = 5


class FunctionExecutorServer:
    """Abstract interface for a Function Executor Server.

    FunctionExecutorServer is a class that executes tasks for a particular function.
    The communication with FunctionExecutorServer is typicall done via gRPC.
    """

    async def create_channel(self, logger: Any) -> grpc.aio.Channel:
        """Creates a new async gRPC channel to the Function Executor Server.

        The channel is in ready state. It can only be used in the same thread where the
        function was called. Caller should close the channel when it's no longer needed.

        Raises Exception if an error occurred."""
        raise NotImplementedError
