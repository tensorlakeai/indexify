import asyncio
import os
import signal
import socket
from typing import Any

from .function_executor_server_factory import (
    FunctionExecutorServerConfiguration,
    FunctionExecutorServerFactory,
)
from .subprocess_function_executor_server import SubprocessFunctionExecutorServer


class SubprocessFunctionExecutorServerFactory(FunctionExecutorServerFactory):
    def __init__(self, verbose_logs: bool) -> None:
        super().__init__()
        self._verbose_logs = verbose_logs

    async def create(
        self, config: FunctionExecutorServerConfiguration, logger: Any
    ) -> SubprocessFunctionExecutorServer:
        logger = logger.bind(module=__name__)
        port: int | None = None

        if len(config.secret_names) > 0:
            logger.warning(
                "Subprocess Function Executor does not support secrets. Please supply secrets as environment variables.",
                secret_names=config.secret_names,
            )

        try:
            port = _find_free_localhost_tcp_port()
            args = [
                f"--executor-id={config.executor_id}",  # use = as executor_id can start with -
                f"--function-executor-id={config.function_executor_id}",
                "--address",
                _server_address(port),
            ]
            if self._verbose_logs:
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


# Function Executors are only listening on localhost so external connections to them are not possible.
# This is a security measure. Also Executor <-> Function Executor communication is always local and
# don't support Function Executors running on a different host.
_FUNCTION_EXECUTOR_SERVER_HOSTNAME = "localhost"


def _server_address(port: int) -> str:
    return f"{_FUNCTION_EXECUTOR_SERVER_HOSTNAME}:{port}"


def _new_process_group() -> None:
    """Creates a new process group with ID equal to the current process PID. POSIX only."""
    os.setpgid(0, 0)


def _find_free_localhost_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((_FUNCTION_EXECUTOR_SERVER_HOSTNAME, 0))
        _, port = sock.getsockname()
        return port
