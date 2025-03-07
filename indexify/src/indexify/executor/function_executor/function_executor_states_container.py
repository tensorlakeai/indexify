import asyncio
from typing import Any, AsyncGenerator, Dict, Optional

from .function_executor_state import FunctionExecutorState
from .function_executor_status import FunctionExecutorStatus
from .metrics.function_executor_state_container import (
    metric_function_executor_states_count,
)


class FunctionExecutorStatesContainer:
    """An asyncio concurrent container for the function executor states."""

    def __init__(self, logger: Any):
        # The fields below are protected by the lock.
        self._lock: asyncio.Lock = asyncio.Lock()
        self._states: Dict[str, FunctionExecutorState] = {}
        self._is_shutdown: bool = False
        self._logger: Any = logger.bind(module=__name__)

    async def get_or_create_state(
        self,
        id: str,
        namespace: str,
        graph_name: str,
        graph_version: str,
        function_name: str,
        image_uri: Optional[str],
    ) -> FunctionExecutorState:
        """Get or create a function executor state with the given ID.

        If the state already exists, it is returned. Otherwise, a new state is created from the supplied task.
        Raises Exception if it's not possible to create a new state at this time."""
        async with self._lock:
            if self._is_shutdown:
                raise RuntimeError(
                    "Function Executor states container is shutting down."
                )

            if id not in self._states:
                state = FunctionExecutorState(
                    id=id,
                    namespace=namespace,
                    graph_name=graph_name,
                    graph_version=graph_version,
                    function_name=function_name,
                    image_uri=image_uri,
                    logger=self._logger,
                )
                self._states[id] = state
                metric_function_executor_states_count.set(len(self._states))

            return self._states[id]

    async def __aiter__(self) -> AsyncGenerator[FunctionExecutorState, None]:
        async with self._lock:
            for state in self._states.values():
                yield state

    async def pop(self, id: str) -> FunctionExecutorState:
        """Removes the state with the given ID and returns it."""
        async with self._lock:
            state = self._states.pop(id)
            metric_function_executor_states_count.set(len(self._states))
            return state

    async def shutdown(self):
        # Function Executors are outside the Executor process
        # so they need to get cleaned up explicitly and reliably.
        async with self._lock:
            self._is_shutdown = True  # No new Function Executor States can be created.
            while self._states:
                id, state = self._states.popitem()
                metric_function_executor_states_count.set(len(self._states))
                # Only ongoing tasks who have a reference to the state already can see it.
                # The state is unlocked while a task is running inside Function Executor.
                async with state.lock:
                    await state.set_status(FunctionExecutorStatus.SHUTDOWN)
                    if state.function_executor is not None:
                        await state.function_executor.destroy()
                        state.function_executor = None
                        # The task running inside the Function Executor will fail because it's destroyed.
