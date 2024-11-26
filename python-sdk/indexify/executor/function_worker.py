import asyncio
import multiprocessing
import os
from typing import Optional

import jsonpickle
import rich
import structlog
from pydantic import BaseModel

from indexify.executor.paths.host import HostPaths
from indexify.function_executor.protocol import (
    FunctionOutput,
    RouterOutput,
    RunFunctionRequest,
    RunFunctionResponse,
)

from .api_objects import Task

logger = structlog.get_logger(module=__name__)

# This would be configurable by customer per function via SDK in the future.
FUNCTION_TIMEOUT_SEC = 60 * 60  # 1 hour


class FunctionWorkerOutput(BaseModel):
    function_output: Optional[FunctionOutput]
    router_output: Optional[RouterOutput]
    stdout: Optional[str]
    stderr: Optional[str]
    reducer: bool
    success: bool


class FunctionWorker:
    def __init__(
        self, executor_cache_path: str, server_addr: str, config_path: Optional[str]
    ):
        self._executor_cache_path: str = executor_cache_path
        self._server_addr: str = server_addr
        self._config_path: Optional[str] = config_path

    async def run(self, request: RunFunctionRequest) -> FunctionWorkerOutput:
        try:
            await _write_request(request)
            proc: asyncio.subprocess.Process = await self._start_process(request)
            proc_stdout, proc_stderr = await _wait_process(request, proc)
            # We want to see process stdout and stderr in Executor logs. Customer func stdout and
            # stderr is not logged because it might contain customer data.
            _log_proc_outputs(request, proc_stdout, proc_stderr)
            response: RunFunctionResponse = _read_response(request)
            return FunctionWorkerOutput(
                function_output=response.function_output,
                router_output=response.router_output,
                stdout=response.stdout,
                stderr=response.stderr,
                reducer=response.is_reducer,
                success=response.success,
            )
        except _FunctionWorkerInternalError:
            # The error message from the process is already logged and we don't want to share internal
            # error messages with the customer.
            return FunctionWorkerOutput(
                function_output=None,
                router_output=None,
                stdout=None,
                stderr="Platform failed to execute the function.",
                reducer=False,
                success=False,
            )

    def shutdown(self) -> None:
        for child_function_executor in multiprocessing.active_children():
            child_function_executor.kill()

    async def _start_process(
        self, request: RunFunctionRequest
    ) -> asyncio.subprocess.Process:
        """Starts a Function Executor process for the given request.

        Raises _FunctionWorkerInternalError if anything fails."""
        try:
            args = [
                "function-executor",
                "--namespace",
                request.namespace,
                "--task-id",
                request.task_id,
                "--executor-cache",
                self._executor_cache_path,
                "--server-addr",
                self._server_addr,
            ]
            if self._config_path is not None:
                args.extend(["--config-path", self._config_path])
            # Run the process with PIPEs so we can control when they output to our stdout, stderr.
            # Othewise we can get a lot of mixed outputs from all child processes in our stdout, stderr.
            return await asyncio.create_subprocess_exec(
                "indexify-cli",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as e:
            _finish_with_internal_error(
                error_message="failed to start Function Executor process",
                request=request,
                exception=e,
            )


async def _wait_process(
    request: RunFunctionRequest, proc: asyncio.subprocess.Process
) -> tuple[str, str]:
    """Waits for the process to finish successfully and returns its stdout and stderr bytes.

    Raises _FunctionWorkerInternalError if anything fails."""
    proc_stdout_str = None
    proc_stderr_str = None
    try:
        proc_stdout, proc_stderr = await asyncio.wait_for(
            proc.communicate(), timeout=FUNCTION_TIMEOUT_SEC
        )
        if proc.returncode is None:
            # Force internal error.
            raise Exception("Function Executor process timed out")
        # At this point proc.communicate() finished and the process exited.
        proc_stdout_str = proc_stdout.decode("utf-8")
        proc_stderr_str = proc_stderr.decode("utf-8")
        if proc.returncode != 0:
            # This happens if our (non-customer) code failed. Customer code failures don't change the error code.
            # Force internal error.
            raise Exception(
                f"Function Executor process finished but has non zero return code {proc.returncode}"
            )
        return (proc_stdout_str, proc_stderr_str)

    except Exception as e:
        # Regardless of the internal error we need to cleanup Function Executor process to avoid resource leaks.
        await _cleanup_proc_no_exceptions(request, proc)
        _log_proc_outputs(
            request=request, proc_stdout=proc_stdout_str, proc_stderr=proc_stderr_str
        )
        _finish_with_internal_error(
            error_message="failed to wait or get stdout/stderr of Function Executor process",
            request=request,
            proc=proc,
            exception=e,
        )


async def _cleanup_proc_no_exceptions(
    request: RunFunctionRequest, proc: asyncio.subprocess.Process
) -> None:
    """Kills and waits for the process to finish.

    Doesn't raise any exceptions. Logs the error if fails."""
    if proc.returncode is not None:
        # The process already exited and was waited() sucessfully.
        return

    try:
        proc.kill()
        await proc.wait()
    except Exception as e:
        logger.error(
            "failed to cleanup Function Executor process",
            exc_info=e,
            **_logger_dict_for(request, proc),
        )


async def _write_request(request: RunFunctionRequest) -> None:
    try:
        file_path = HostPaths.task_run_function_request(request.task_id)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(jsonpickle.encode(request).encode("utf-8"))
    except Exception as e:
        _finish_with_internal_error(
            error_message="failed to write Function Executor request to host filesystem",
            request=request,
            exception=e,
        )


def _read_response(request: RunFunctionRequest) -> RunFunctionResponse:
    try:
        with open(HostPaths.task_run_function_response(request.task_id), "rb") as f:
            return jsonpickle.decode(f.read().decode("utf-8"))
    except Exception as e:
        _finish_with_internal_error(
            error_message="failed reading Function Executor response",
            request=request,
            exception=e,
        )


def _log_proc_outputs(
    request: RunFunctionRequest, proc_stdout: str, proc_stderr: str
) -> None:
    try:
        # Stdout and stderr are pre-formatted by the process with colors. Use rich.print to keep the colors.
        if proc_stdout:
            rich.print(proc_stdout)
        if proc_stderr:
            rich.print(proc_stdout)
    except Exception as e:
        _finish_with_internal_error(
            error_message="failed logging Function Executor process stdout and stderr",
            request=request,
            exception=e,
        )


def _finish_with_internal_error(
    error_message: str,
    request: RunFunctionRequest,
    proc: Optional[asyncio.subprocess.Process],
    exception: Optional[Exception],
) -> None:
    """Logs the internal error message to help debugging and raises _FunctionWorkerInternalError."""
    logger.error(error_message, exc_info=exception, **_logger_dict_for(request, proc))
    raise _FunctionWorkerInternalError()


def _logger_dict_for(
    request: RunFunctionRequest, proc: Optional[asyncio.subprocess.Process]
) -> dict[str, str]:
    labels = {
        "task_id": request.task_id,
        "function": request.function_name,
    }
    if proc is not None:
        labels["function_executor_pid"] = str(proc.pid)
        labels["function_executor_return_code"] = str(proc.returncode)
    return labels


class _FunctionWorkerInternalError(Exception):
    pass
