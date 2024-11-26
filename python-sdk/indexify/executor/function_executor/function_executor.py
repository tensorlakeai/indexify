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

    def create_channel(self) -> grpc.Channel:
        """Returns a new gRPC channel to the Function Executor.

        Raises Exception if an error occurred."""
        raise NotImplementedError
