import asyncio
from typing import Any, Optional

import grpc

from indexify.function_executor.proto.configuration import GRPC_CHANNEL_OPTIONS

from .function_executor import (
    FUNCTION_EXECUTOR_READY_TIMEOUT_SEC,
    FunctionExecutor,
)


class ProcessFunctionExecutor(FunctionExecutor):
    """A FunctionExecutor that runs in a separate host process."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        port: int,
        address: str,
        logger: Any,
        state: Optional[Any] = None,
    ):
        self._proc = process
        self._port = port
        self._address = address
        self._logger = logger.bind(module=__name__)
        self._channel: Optional[grpc.aio.Channel] = None
        self._state: Optional[Any] = state

    async def channel(self) -> grpc.aio.Channel:
        # Not thread safe but async safe because we don't await.
        if self._channel is not None:
            return self._channel

        channel: Optional[grpc.aio.Channel] = None
        try:
            channel = grpc.aio.insecure_channel(
                self._address, options=GRPC_CHANNEL_OPTIONS
            )
            await asyncio.wait_for(
                channel.channel_ready(),
                timeout=FUNCTION_EXECUTOR_READY_TIMEOUT_SEC,
            )
            # Check if another channel was created by a concurrent coroutine.
            # Not thread safe but async safe because we never overwrite non-None self._channel.
            if self._channel is not None:
                # Don't close and overwrite existing channel because it might be used for RPCs already.
                await channel.close()
                return self._channel
            else:
                self._channel = channel
                return channel
        except Exception:
            if channel is not None:
                await channel.close()
            self._logger.error(
                f"failed to connect to the gRPC server at {self._address} within {FUNCTION_EXECUTOR_READY_TIMEOUT_SEC} seconds"
            )
            raise

    def state(self) -> Optional[Any]:
        return self._state
