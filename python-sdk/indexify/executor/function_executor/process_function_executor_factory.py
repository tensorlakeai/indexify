import asyncio
from typing import Any, Optional

from .function_executor_factory import FunctionExecutorFactory
from .process_function_executor import ProcessFunctionExecutor


class ProcessFunctionExecutorFactory(FunctionExecutorFactory):
    def __init__(
        self,
        indexify_server_address: str,
        development_mode: bool,
        config_path: Optional[str],
    ):
        self._indexify_server_address: str = indexify_server_address
        self._development_mode: bool = development_mode
        self._config_path: Optional[str] = config_path
        # Registred ports range end at 49151. We start from 50000 to hopefully avoid conflicts.
        self._free_ports = set(range(50000, 51000))

    async def create(
        self, logger: Any, state: Optional[Any] = None
    ) -> ProcessFunctionExecutor:
        logger = logger.bind(module=__name__)
        port: Optional[int] = None

        try:
            port = self._allocate_port()
            args = [
                "function-executor",
                "--function-executor-server-address",
                _server_address(port),
                "--indexify-server-address",
                self._indexify_server_address,
            ]
            if self._development_mode:
                args.append("--dev")
            if self._config_path is not None:
                args.extend(["--config-path", self._config_path])
            # Run the process with our stdout, stderr. We want to see process logs and exceptions in our process output.
            # This is useful for dubugging. Customer function stdout and stderr is captured and returned in the response
            # so we won't see it in our process outputs. This is the right behavior as customer function stdout and stderr
            # contains private customer data.
            proc: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                "indexify-cli",
                *args,
            )
            return ProcessFunctionExecutor(
                process=proc,
                port=port,
                address=_server_address(port),
                logger=logger,
                state=state,
            )
        except Exception as e:
            if port is not None:
                self._release_port(port)
            logger.error(
                "failed starting a new Function Executor process at port {port}",
                exc_info=e,
            )
            raise

    async def destroy(self, executor: ProcessFunctionExecutor, logger: Any) -> None:
        proc: asyncio.subprocess.Process = executor._proc
        port: int = executor._port
        logger = logger.bind(
            module=__name__,
            pid=proc.pid,
            port=port,
        )

        try:
            if proc.returncode is not None:
                # The process already exited and was waited() sucessfully.
                return

            proc.kill()
            await proc.wait()
        except Exception as e:
            logger.error(
                "failed to cleanup Function Executor process",
                exc_info=e,
            )
        finally:
            self._release_port(port)
            if executor._channel is not None:
                await executor._channel.close()

    def _allocate_port(self) -> int:
        # No asyncio.Lock is required here because this operation never awaits
        # and it is always called from the same thread where the event loop is running.
        return self._free_ports.pop()

    def _release_port(self, port: int) -> None:
        # No asyncio.Lock is required here because this operation never awaits
        # and it is always called from the same thread where the event loop is running.
        self._free_ports.add(port)


def _server_address(port: int) -> str:
    return f"localhost:{port}"
