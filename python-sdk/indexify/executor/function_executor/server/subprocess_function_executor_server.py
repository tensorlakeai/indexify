import asyncio
from typing import Any

import grpc

from indexify.function_executor.proto.configuration import GRPC_CHANNEL_OPTIONS

from .function_executor_server import FunctionExecutorServer


class SubprocessFunctionExecutorServer(FunctionExecutorServer):
    """A FunctionExecutorServer that runs in a child process."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        port: int,
        address: str,
    ):
        self._proc = process
        self._port = port
        self._address = address

    async def create_channel(self, logger: Any) -> grpc.aio.Channel:
        return grpc.aio.insecure_channel(self._address, options=GRPC_CHANNEL_OPTIONS)
