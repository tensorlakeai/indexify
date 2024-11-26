import asyncio
import multiprocessing
from typing import Optional, Tuple

import grpc
import structlog

from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    RouterOutput,
    RunTaskRequest,
    RunTaskResponse,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

logger = structlog.get_logger(module=__name__)

FUNCTION_EXECUTOR_STARTUP_TIMEOUT_SEC = 5
FUNCTION_EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 5


class FunctionWorkerOutput:
    def __init__(
        self,
        function_output: Optional[FunctionOutput] = None,
        router_output: Optional[RouterOutput] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        reducer: bool = False,
        success: bool = False,
    ):
        self.function_output = function_output
        self.router_output = router_output
        self.stdout = stdout
        self.stderr = stderr
        self.reducer = reducer
        self.success = success


class FunctionWorker:
    def __init__(self, indexify_server_address: str, config_path: Optional[str]):
        self._indexify_server_address: str = indexify_server_address
        self._config_path: Optional[str] = config_path
        # Registred ports range end at 49151. We start from 50000 to hopefully avoid conflicts.
        self._free_ports = set(range(50000, 51000))

    async def run(self, request: RunTaskRequest) -> FunctionWorkerOutput:
        proc: Optional[asyncio.subprocess.Process] = None
        port: Optional[int] = None

        try:
            proc, port = await self._start_process()
            with self._rpc_channel(port) as channel:
                response: RunTaskResponse = FunctionExecutorStub(channel).RunTask(
                    request
                )
                return _worker_output(response)
        except Exception as e:
            # The error message from the process is already logged and we don't want to share internal
            # error messages with the customer.
            logger.error(
                "failed running function in a new Function Executor process",
                exc_info=e,
                **_logger_dict_for(request, proc),
            )
            return FunctionWorkerOutput(
                function_output=None,
                router_output=None,
                stdout=None,
                stderr="Platform failed to execute the function.",
                reducer=False,
                success=False,
            )
        finally:
            await self._cleanup_proc_no_exceptions(
                request=request, proc=proc, port=port
            )

    def shutdown(self) -> None:
        for child_function_executor in multiprocessing.active_children():
            child_function_executor.kill()

    async def _start_process(self) -> Tuple[asyncio.subprocess.Process, int]:
        """Starts a Function Executor process for the given request."""
        port: Optional[int] = None

        try:
            port = self._free_ports.pop()
            args = [
                "function-executor",
                "--function-executor-server-address",
                _server_address(port),
                "--indexify-server-address",
                self._indexify_server_address,
            ]
            if self._config_path is not None:
                args.extend(["--config-path", self._config_path])
            # Run the process with our stdout, stderr. We want to see process logs and exceptions in our process output.
            # This is useful for dubugging. Customer function stdout and stderr is captured and returned in the response
            # so we won't see it in our process outputs. This is the right behavior as customer function stdout and stderr
            # contains private customer data.
            return (
                await asyncio.create_subprocess_exec(
                    "indexify-cli",
                    *args,
                ),
                port,
            )
        except Exception:
            if port is not None:
                self._free_ports.add(port)
            logger.error(
                "failed starting a new Function Executor process at port {port}"
            )
            raise

    async def _cleanup_proc_no_exceptions(
        self,
        request: RunTaskRequest,
        proc: Optional[asyncio.subprocess.Process],
        port: Optional[int],
    ) -> None:
        """Kills and waits for the process to finish.

        Doesn't raise any exceptions. Logs the error if fails."""
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
                **_logger_dict_for(request, proc),
            )
        finally:
            if port is not None:
                self._free_ports.add(port)

    def _rpc_channel(self, port: int) -> grpc.Channel:
        channel: grpc.Channel = grpc.insecure_channel(_server_address(port))
        try:
            # This is not asyncio.Future but grpc.Future. It has a different interface.
            grpc.channel_ready_future(channel).result(
                timeout=FUNCTION_EXECUTOR_STARTUP_TIMEOUT_SEC
            )
            return channel
        except Exception:
            channel.close()
            logger.error(
                f"Failed to connect to the gRPC server within {FUNCTION_EXECUTOR_STARTUP_TIMEOUT_SEC} seconds"
            )
            raise


def _logger_dict_for(
    request: RunTaskRequest, proc: Optional[asyncio.subprocess.Process]
) -> dict[str, str]:
    labels = {
        "task_id": request.task_id,
        "function": request.function_name,
    }
    if proc is not None:
        labels["function_executor_pid"] = str(proc.pid)
        labels["function_executor_return_code"] = str(proc.returncode)
    return labels


def _server_address(port: int) -> str:
    return f"localhost:{port}"


def _worker_output(response: RunTaskResponse) -> FunctionWorkerOutput:
    required_fields = [
        "stdout",
        "stderr",
        "is_reducer",
        "success",
    ]

    for field in required_fields:
        if not response.HasField(field):
            raise ValueError(f"Response is missing required field: {field}")

    output = FunctionWorkerOutput(
        stdout=response.stdout,
        stderr=response.stderr,
        reducer=response.is_reducer,
        success=response.success,
    )

    if response.HasField("function_output"):
        output.function_output = response.function_output
    if response.HasField("router_output"):
        output.router_output = response.router_output

    return output
