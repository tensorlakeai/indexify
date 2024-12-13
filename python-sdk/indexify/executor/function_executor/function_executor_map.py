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
        # We have to do all operations under this lock because we need to ensure
        # that we don't create more Function Executors than required. This is important
        # e.g. when a Function Executor is using the only available GPU on the machine.
        # We can get rid of this locking in the future once we assing GPUs explicitly to Function Executors.
        # Running the full test suite with all this locking removed doesn't make it run faster,
        # so it looks like this full locking doesn't really result in any performance penalty so far.
        self._executors_lock = asyncio.Lock()

    async def get_or_create(
        self,
        id: str,
        initialize_request: InitializeRequest,
        initial_state: Any,
        logger: Any,
    ) -> FunctionExecutor:
        """Returns a FunctionExecutor for the given ID.

        If the FunctionExecutor for the given ID doesn't exist then it will be created and initialized.
        Raises an exception if the FunctionExecutor creation or initialization failed.
        """
        async with self._executors_lock:
            # Use existing Function Executor if it's already initialized.
            if id in self._executors:
                return self._executors[id]

            executor: Optional[FunctionExecutor] = None
            try:
                executor = await self._factory.create(logger, state=initial_state)
                channel: grpc.aio.Channel = await executor.channel()
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize_response: InitializeResponse = await stub.initialize(
                    initialize_request
                )
                if not initialize_response.success:
                    raise Exception("initialize RPC failed at function executor")
            except Exception:
                if executor is not None:
                    await self._factory.destroy(executor=executor, logger=logger)
                # Function Executor creation or initialization failed.
                raise

            self._executors[id] = executor
            return executor

    async def delete(
        self, id: str, function_executor: FunctionExecutor, logger: Any
    ) -> None:
        """Deletes the FunctionExecutor for the given ID.

        Does nothing if the FunctionExecutor for the given ID doesn't exist or was already deleted.
        """
        async with self._executors_lock:
            if self._executors[id] != function_executor:
                # Function Executor was already deleted or replaced and the caller is not aware of this.
                return
            del self._executors[id]
            await self._factory.destroy(executor=function_executor, logger=logger)

    async def clear(self, logger):
        async with self._executors_lock:
            while self._executors:
                id, function_executor = self._executors.popitem()
                await self._factory.destroy(function_executor, logger)
