import asyncio
from typing import Any, Dict, Optional

import grpc

from indexify.function_executor.proto.function_executor_pb2 import (
    InitializeRequest,
    InitializeResponse,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

from .function_executor import FunctionExecutor
from .function_executor_factory import FunctionExecutorFactory


class FunctionExecutorMap:
    """A map of ID => FunctionExecutor.

    The map is safe to use by multiple couroutines running in event loop on the same thread
    but it's not thread safe (can't be used from different threads concurrently)."""

    def __init__(self, factory: FunctionExecutorFactory):
        self._factory = factory
        # Map of initialized Function executors ready to run tasks.
        # function ID -> FunctionExecutor
        self._executors: Dict[str, FunctionExecutor] = {}

    async def get_or_create(
        self, id: str, initialize_request: InitializeRequest, logger: Any
    ) -> FunctionExecutor:
        """Returns a FunctionExecutor for the given ID.

        If the FunctionExecutor for the given ID doesn't exist then it will be created and initialized.
        Raises an exception if the FunctionExecutor creation or initialization failed.
        """
        # Use existing Function Executor if it's already initialized.
        # No need to use a Lock because we don't await while doing actions that depend on previously read dict state and while modifying it.
        if id in self._executors:
            return self._executors[id]

        # Create a new Function Executor without blocking other tasks on the lock.
        executor: Optional[FunctionExecutor] = None
        try:
            executor = await self._factory.create(logger)
            channel: grpc.aio.Channel = await executor.channel()
            stub: FunctionExecutorStub = FunctionExecutorStub(channel)
            initialize_response: InitializeResponse = await stub.Initialize(
                initialize_request
            )
            if not initialize_response.success:
                raise Exception("initialize RPC failed at function executor")
        except Exception:
            if executor is not None:
                await self._factory.destroy(executor=executor, logger=logger)
            # Function Executor creation or initialization failed.
            raise

        # Save the newly initialized Function Executor in the map if it wasn't concurrently created for another task.
        # No need to use a Lock because we don't await while doing actions that depend on previously read dict state and while modifying it.
        if id in self._executors:
            # Schedule the new Function Executor for destruction and use the concurrently created one.
            # We don't need to wait for the completion so we use create_task.
            asyncio.create_task(self._factory.destroy(executor=executor, logger=logger))
            return self._executors[id]
        self._executors[id] = executor
        return executor

    async def delete_and_destroy(self, id: str, logger: Any) -> None:
        # No need to use a Lock because we don't await while doing actions that depend on previously read dict state and while modifying it.
        executor = self._executors[id]
        del self._executors[id]
        await self._factory.destroy(executor=executor, logger=logger)

    async def clear(self, logger):
        # No need to use a Lock because we don't await while doing actions that depend on previously read dict state and while modifying it.
        while self._executors:
            id, function_executor = self._executors.popitem()
            await self._factory.destroy(function_executor, logger)
