import asyncio
from typing import Optional

from indexify.task_scheduler.proto.task_scheduler_pb2 import FunctionExecutorStatus

from .function_executor import FunctionExecutor
from .metrics.function_executor_state import (
    metric_function_executor_state_not_locked_errors,
)


class FunctionExecutorState:
    """State of a Function Executor with a particular ID.

    The Function Executor might not exist, i.e. not yet created or destroyed.
    This object represents all such states. Any state modification must be done
    under the lock.
    """

    def __init__(
        self,
        id: str,
        namespace: str,
        graph_name: str,
        graph_version: str,
        function_name: str,
        image_uri: Optional[str],
    ):
        # Read only fields.
        self.id: str = id
        self.namespace: str = namespace
        self.graph_name: str = graph_name
        self.function_name: str = function_name
        self.image_uri: Optional[str] = image_uri
        # The lock must be held while modifying the fields below.
        self.lock: asyncio.Lock = asyncio.Lock()
        self.graph_version: str = graph_version
        self.is_shutdown: bool = False
        # Set to True if a Function Executor health check ever failed.
        self.health_check_failed: bool = False
        # TODO: remove fields that duplicate this status field.
        self.status: FunctionExecutorStatus = (
            FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STOPPED
        )
        self.function_executor: Optional[FunctionExecutor] = None
        self.running_tasks: int = 0
        self.running_tasks_change_notifier: asyncio.Condition = asyncio.Condition(
            lock=self.lock
        )

    def increment_running_tasks(self) -> None:
        """Increments the number of running tasks.

        The caller must hold the lock.
        """
        self.check_locked()
        self.running_tasks += 1
        self.running_tasks_change_notifier.notify_all()

    def decrement_running_tasks(self) -> None:
        """Decrements the number of running tasks.

        The caller must hold the lock.
        """
        self.check_locked()
        self.running_tasks -= 1
        self.running_tasks_change_notifier.notify_all()

    async def wait_running_tasks_less(self, value: int) -> None:
        """Waits until the number of running tasks is less than the supplied value.

        The caller must hold the lock.
        """
        self.check_locked()
        while self.running_tasks >= value:
            await self.running_tasks_change_notifier.wait()

    async def destroy_function_executor(self) -> None:
        """Destroys the Function Executor if it exists.

        The caller must hold the lock."""
        self.check_locked()
        if self.function_executor is not None:
            await self.function_executor.destroy()
            self.function_executor = None

    async def shutdown(self) -> None:
        """Shuts down the state.

        Called only during Executor shutdown so it's okay to fail all running and pending
        Function Executor tasks. The state is not valid anymore after this call.
        The caller must hold the lock.
        """
        self.check_locked()
        # Pending tasks will not create a new Function Executor and won't run.
        self.is_shutdown = True
        await self.destroy_function_executor()

    def check_locked(self) -> None:
        """Raises an exception if the lock is not held."""
        if not self.lock.locked():
            metric_function_executor_state_not_locked_errors.inc()
            raise RuntimeError("The FunctionExecutorState lock must be held.")
