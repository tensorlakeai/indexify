import asyncio
import os
import signal
import socket
from typing import Any, Optional

from .function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from .subprocess_function_executor_server import SubprocessFunctionExecutorServer


def get_free_tcp_port(iface_name="localhost") -> int:
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind((iface_name, 0))
    _, port = tcp.getsockname()
    tcp.close()
    return port


class SubprocessFunctionExecutorServerFactory(FunctionExecutorServerFactory):
    async def create(
        self, config: FunctionExecutorServerConfiguration, logger: Any
    ) -> SubprocessFunctionExecutorServer:
        logger = logger.bind(module=__name__)
        port: Optional[int] = None

        if len(config.secret_names) > 0:
            logger.warning(
                "Subprocess Function Executor does not support secrets. Please supply secrets as environment variables.",
                secret_names=config.secret_names,
            )

        try:
            port = get_free_tcp_port()
            logger.info("allocated function executor port", port=port)
            args = [
                f"--executor-id={config.executor_id}",  # use = as executor_id can start with -
                "--address",
                _server_address(port),
            ]
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


def _server_address(port: int) -> str:
    return f"localhost:{port}"


def _new_process_group() -> None:
    """Creates a new process group with ID equal to the current process PID. POSIX only."""
    os.setpgid(0, 0)
