import asyncio
from typing import AsyncGenerator, Dict

from ..api_objects import Task
from .function_executor_state import FunctionExecutorState
from .metrics.function_executor_state_container import (
    metric_function_executor_states_count,
)


class FunctionExecutorStatesContainer:
    """An asyncio concurrent container for the function executor states."""

    def __init__(self):
        # The fields below are protected by the lock.
        self._lock: asyncio.Lock = asyncio.Lock()
        self._states: Dict[str, FunctionExecutorState] = {}
        self._is_shutdown: bool = False

    async def get_or_create_state(self, id: str, task: Task) -> FunctionExecutorState:
        """Get or create a function executor state with the given ID.

        If the state already exists, it is returned. Otherwise, a new state is created from the supplied task.
        Raises Exception if it's not possible to create a new state at this time."""
        async with self._lock:
            if self._is_shutdown:
                raise RuntimeError("Task runner is shutting down.")

            if id not in self._states:
                state = FunctionExecutorState(
                    namespace=task.namespace,
                    graph_name=task.compute_graph,
                    graph_version=task.graph_version,
                    function_name=task.compute_fn,
                )
                self._states[id] = state
                metric_function_executor_states_count.set(len(self._states))

            return self._states[id]

    async def __aiter__(self) -> AsyncGenerator[FunctionExecutorState, None]:
        async with self._lock:
            for state in self._states.values():
                yield state

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
                    await state.shutdown()
                    # The task running inside the Function Executor will fail because it's destroyed.
