import asyncio
from typing import Any, override

import grpc

from .client_configuration import GRPC_CHANNEL_OPTIONS
from .function_executor_server import (
    FunctionExecutorServer,
    FunctionExecutorServerStatus,
)


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

    @override
    async def create_channel(self, logger: Any) -> grpc.aio.Channel:
        return grpc.aio.insecure_channel(self._address, options=GRPC_CHANNEL_OPTIONS)

    @override
    async def status(self) -> FunctionExecutorServerStatus:
        """
        Returns information about the function executor.
        If the returncode is None, the process is still running.
        """

        if self._proc.returncode is None:
            return FunctionExecutorServerStatus(True, False)
        else:
            return FunctionExecutorServerStatus(False, await self._check_oom())

    async def _check_oom(self) -> bool:
        """
        Returns True if the process with the given pid was killed due to an out-of-memory error.
        """
        oom_pattern = f"Out of memory: Killed process {self._proc.pid}"

        try:
            proc = await asyncio.create_subprocess_shell(
                f'dmesg | grep "{oom_pattern}"',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0 and stdout.strip():
                return True
        except (FileNotFoundError, OSError):
            pass  # dmesg failed or not available

        return False
