from typing import Any

from .function_executor import FunctionExecutor


class FunctionExecutorFactory:
    """Abstract class for creating function executors."""

    async def create(self, logger: Any) -> FunctionExecutor:
        """Creates a new function executor.

        Args:
            logger: logger to be used during the function."""
        raise NotImplementedError()

    async def destroy(self, executor: FunctionExecutor, logger: Any) -> None:
        """Destroys the FunctionExecutor and release all its resources.

        Args:
            logger: logger to be used during the function.
        Function Executor and customer code running inside of it is not notified about the destruction.
        Never raises any Exceptions."""
        raise NotImplementedError
