import asyncio
from typing import Any

import grpc

from .function_executor import (
    FUNCTION_EXECUTOR_READY_TIMEOUT_SEC,
    FunctionExecutor,
)


class ProcessFunctionExecutor(FunctionExecutor):
    """A FunctionExecutor that runs in a separate host process."""

    def __init__(
        self, process: asyncio.subprocess.Process, port: int, address: str, logger: Any
    ):
        self._proc = process
        self._port = port
        self._address = address
        self._logger = logger.bind(module=__name__)

    def create_channel(self) -> grpc.Channel:
        channel: grpc.Channel = grpc.insecure_channel(self._address)
        try:
            # This is not asyncio.Future but grpc.Future. It has a different interface.
            grpc.channel_ready_future(channel).result(
                timeout=FUNCTION_EXECUTOR_READY_TIMEOUT_SEC
            )
            return channel
        except Exception:
            channel.close()
            self._logger.error(
                f"failed to connect to the gRPC server within {FUNCTION_EXECUTOR_READY_TIMEOUT_SEC} seconds"
            )
            raise
