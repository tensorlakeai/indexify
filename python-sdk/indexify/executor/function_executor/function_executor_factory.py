from typing import Any, Optional

from .function_executor import FunctionExecutor


class FunctionExecutorFactory:
    """Abstract class for creating function executors."""

    async def create(
        self, logger: Any, state: Optional[Any] = None
    ) -> FunctionExecutor:
        """Creates a new FunctionExecutor.

        Args:
            logger: logger to be used during the function.
            state: state to be stored in the FunctionExecutor."""
        raise NotImplementedError()

    async def destroy(self, executor: FunctionExecutor, logger: Any) -> None:
        """Destroys the FunctionExecutor and release all its resources.

        Args:
            logger: logger to be used during the function.
        FunctionExecutor and customer code running inside of it are not notified about the destruction.
        Never raises any Exceptions."""
        raise NotImplementedError
