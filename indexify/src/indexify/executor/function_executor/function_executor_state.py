import asyncio
from typing import Any, List, Optional

from .function_executor import FunctionExecutor
from .function_executor_status import FunctionExecutorStatus, is_status_change_allowed
from .metrics.function_executor_state import (
    metric_function_executor_state_not_locked_errors,
    metric_function_executors_with_status,
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
        logger: Any,
    ):
        # Read only fields.
        self.id: str = id
        self.namespace: str = namespace
        self.graph_name: str = graph_name
        self.function_name: str = function_name
        self.image_uri: Optional[str] = image_uri
        self._logger: Any = logger.bind(
            module=__name__,
            function_executor_id=id,
            namespace=namespace,
            graph_name=graph_name,
            graph_version=graph_version,
            function_name=function_name,
            image_uri=image_uri,
        )
        # The lock must be held while modifying the fields below.
        self.lock: asyncio.Lock = asyncio.Lock()
        # TODO: Move graph_version to immutable fields once we migrate to gRPC State Reconciler.
        self.graph_version: str = graph_version
        self.status: FunctionExecutorStatus = FunctionExecutorStatus.DESTROYED
        self.status_change_notifier: asyncio.Condition = asyncio.Condition(
            lock=self.lock
        )
        self.function_executor: Optional[FunctionExecutor] = None
        metric_function_executors_with_status.labels(status=self.status.name).inc()

    async def wait_status(self, allowlist: List[FunctionExecutorStatus]) -> None:
        """Waits until Function Executor status reaches one of the allowed values.

        The caller must hold the lock.
        """
        self.check_locked()
        while self.status not in allowlist:
            await self.status_change_notifier.wait()

    async def set_status(self, new_status: FunctionExecutorStatus) -> None:
        """Sets the status of the Function Executor.

        The caller must hold the lock.
        Raises ValueError if the status change is not allowed.
        """
        self.check_locked()
        if is_status_change_allowed(self.status, new_status):
            self._logger.info(
                "function executor status changed",
                old_status=self.status.name,
                new_status=new_status.name,
            )
            metric_function_executors_with_status.labels(status=self.status.name).dec()
            metric_function_executors_with_status.labels(status=new_status.name).inc()
            self.status = new_status
            self.status_change_notifier.notify_all()
        else:
            raise ValueError(
                f"Invalid status change from {self.status} to {new_status}"
            )

    async def destroy_function_executor(self) -> None:
        """Destroys the Function Executor if it exists.

        The caller must hold the lock.
        """
        self.check_locked()
        await self.set_status(FunctionExecutorStatus.DESTROYING)
        if self.function_executor is not None:
            await self.function_executor.destroy()
            self.function_executor = None
        await self.set_status(FunctionExecutorStatus.DESTROYED)

    def check_locked(self) -> None:
        """Raises an exception if the lock is not held."""
        if not self.lock.locked():
            metric_function_executor_state_not_locked_errors.inc()
            raise RuntimeError("The FunctionExecutorState lock must be held.")
