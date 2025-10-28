import asyncio
from typing import Any

from indexify.proto.executor_api_pb2 import (
    FunctionCallWatch,
)


class FunctionCallWatchDispatcher:
    """A wrapper over ExecutorStateReconciler that allows to avoid circular dependencies."""

    def __init__(self, state_reconciler: Any):
        # Uses Any instead of ExecutorStateReconciler
        self._state_reconciler: Any = state_reconciler

    def add_function_call_watch(
        self, watch: FunctionCallWatch, result_queue: asyncio.Queue
    ) -> None:
        return self._state_reconciler.add_function_call_watch(watch, result_queue)

    def remove_function_call_watch(
        self, watch: FunctionCallWatch, result_queue: asyncio.Queue
    ) -> None:
        return self._state_reconciler.remove_function_call_watch(watch, result_queue)
