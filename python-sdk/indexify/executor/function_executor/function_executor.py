from typing import Any, Optional

import grpc

# Timeout for Function Executor startup in seconds.
# The timeout is counted from the moment when the Function Executor environment
# is fully prepared and the Function Executor gets started.
FUNCTION_EXECUTOR_READY_TIMEOUT_SEC = 5


class FunctionExecutor:
    """Abstract interface for a FunctionExecutor.

    FunctionExecutor is a class that executes tasks for a particular function.
    FunctionExecutor implements the gRPC server that listens for incoming tasks.
    """

    async def channel(self) -> grpc.aio.Channel:
        """Returns a async gRPC channel to the Function Executor.

        The channel is in ready state and can be used for all gRPC communication with the Function Executor
        and can be shared among coroutines running in the same event loop in the same thread. Users should
        not close the channel as it's reused for all requests.
        Raises Exception if an error occurred."""
        raise NotImplementedError

    def state(self) -> Optional[Any]:
        """Returns optional state object.

        The state object can be used to associate any data with the Function Executor.
        """
        raise NotImplementedError
