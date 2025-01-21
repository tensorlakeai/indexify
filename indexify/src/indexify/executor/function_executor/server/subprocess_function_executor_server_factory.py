import asyncio
import os
import signal
from typing import Any, List, Optional

from .function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from .subprocess_function_executor_server import SubprocessFunctionExecutorServer


class SubprocessFunctionExecutorServerFactory(FunctionExecutorServerFactory):
    def __init__(
        self,
        development_mode: bool,
        server_ports: range,
    ):
        self._development_mode: bool = development_mode
        self._free_ports: List[int] = list(reversed(server_ports))

    async def create(
        self, config: FunctionExecutorServerConfiguration, logger: Any
    ) -> SubprocessFunctionExecutorServer:
        logger = logger.bind(module=__name__)
        port: Optional[int] = None

        try:
            port = self._allocate_port()
            args = [
                "--address",
                _server_address(port),
            ]
            if self._development_mode:
                args.append("--dev")
            # Run the process with our stdout, stderr. We want to see process logs and exceptions in our process output.
            # This is useful for dubugging. Customer function stdout and stderr is captured and returned in the response
            # so we won't see it in our process outputs. This is the right behavior as customer function stdout and stderr
            # contains private customer data.
            proc: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                "function-executor",
                *args,
                # TODO: pass `process_group=0` instead of the depricated `preexec_fn` once we only support Python 3.11+.
                preexec_fn=_new_process_group,
            )
            return SubprocessFunctionExecutorServer(
                process=proc,
                port=port,
                address=_server_address(port),
            )
        except Exception as e:
            if port is not None:
                self._release_port(port)
            logger.error(
                "failed starting a new Function Executor process at port {port}",
                exc_info=e,
            )
            raise

    async def destroy(
        self, server: SubprocessFunctionExecutorServer, logger: Any
    ) -> None:
        proc: asyncio.subprocess.Process = server._proc
        port: int = server._port
        logger = logger.bind(
            module=__name__,
            pid=proc.pid,
            port=port,
        )

        try:
            if proc.returncode is not None:
                # The process already exited and was waited() sucessfully.
                return

            if os.name == "posix":
                # On POSIX systems, we can kill the whole process group so processes forked by customer code are also killed.
                # This should be done before proc.kill because PG processes get their own PG when their PG leader dies.
                os.killpg(proc.pid, signal.SIGKILL)
            proc.kill()
            await proc.wait()
        except Exception as e:
            logger.error(
                "failed to cleanup Function Executor process",
                exc_info=e,
            )
        finally:
            self._release_port(port)

    def _allocate_port(self) -> int:
        # No asyncio.Lock is required here because this operation never awaits
        # and it is always called from the same thread where the event loop is running.
        return self._free_ports.pop()

    def _release_port(self, port: int) -> None:
        # No asyncio.Lock is required here because this operation never awaits
        # and it is always called from the same thread where the event loop is running.
        #
        # Prefer port reuse to repro as many possible issues deterministically as possible.
        self._free_ports.append(port)


def _server_address(port: int) -> str:
    return f"localhost:{port}"


def _new_process_group() -> None:
    """Creates a new process group with ID equal to the current process PID. POSIX only."""
    os.setpgid(0, 0)
