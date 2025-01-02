import asyncio
from typing import Any, List, Optional, Tuple

import nanoid
from httpx import Timeout

from indexify.common_util import get_httpx_client
from indexify.executor.api_objects import RouterOutput, Task, TaskResult
from indexify.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
)

from .task_runner import TaskOutput


# https://github.com/psf/requests/issues/1081#issuecomment-428504128
class ForceMultipartDict(dict):
    def __bool__(self):
        return True


FORCE_MULTIPART = ForceMultipartDict()
UTF_8_CONTENT_TYPE = "application/octet-stream"


class TaskOutputSummary:
    def __init__(self):
        self.output_count: int = 0
        self.output_total_bytes: int = 0
        self.router_output_count: int = 0
        self.stdout_count: int = 0
        self.stdout_total_bytes: int = 0
        self.stderr_count: int = 0
        self.stderr_total_bytes: int = 0
        self.total_bytes: int = 0


class TaskReporter:
    def __init__(
        self, base_url: str, executor_id: str, config_path: Optional[str] = None
    ):
        self._base_url = base_url
        self._executor_id = executor_id
        # Use thread-safe sync client due to issues with async client.
        # Async client attempts to use connections it already closed.
        # See e.g. https://github.com/encode/httpx/issues/2337.
        # Creating a new async client for each request fixes this but it
        # results in not reusing established TCP connections to server.
        self._client = get_httpx_client(config_path, make_async=False)

    async def report(self, output: TaskOutput, logger: Any):
        """Reports result of the supplied task."""
        logger = logger.bind(module=__name__)
        task_result, output_files, output_summary = self._process_task_output(output)
        task_result_data = task_result.model_dump_json(exclude_none=True)

        logger.info(
            "reporting task outcome",
            total_bytes=output_summary.total_bytes,
            total_files=output_summary.output_count
            + output_summary.stdout_count
            + output_summary.stderr_count,
            output_files=output_summary.output_count,
            output_bytes=output_summary.total_bytes,
            router_output_count=output_summary.router_output_count,
            stdout_bytes=output_summary.stdout_total_bytes,
            stderr_bytes=output_summary.stderr_total_bytes,
        )

        kwargs = {
            "data": {"task_result": task_result_data},
            # Use httpx default timeout of 5s for all timeout types.
            # For read timeouts, use 5 minutes to allow for large file uploads.
            "timeout": Timeout(
                5.0,
                read=5.0 * 60,
            ),
            "files": output_files if len(output_files) > 0 else FORCE_MULTIPART,
        }
        # Run in a separate thread to not block the main event loop.
        response = await asyncio.to_thread(
            self._client.post, url=f"{self._base_url}/internal/ingest_files", **kwargs
        )

        try:
            response.raise_for_status()
        except Exception as e:
            # Caller catches and logs the exception.
            raise Exception(
                "failed to report task outcome. "
                f"Response code: {response.status_code}. "
                f"Response text: '{response.text}'."
            ) from e

    def _process_task_output(
        self, output: TaskOutput
    ) -> Tuple[TaskResult, List[Any], TaskOutputSummary]:
        task_result = TaskResult(
            outcome="failure",
            namespace=output.task.namespace,
            compute_graph=output.task.compute_graph,
            compute_fn=output.task.compute_fn,
            invocation_id=output.task.invocation_id,
            executor_id=self._executor_id,
            task_id=output.task.id,
        )
        output_files: List[Any] = []
        summary: TaskOutputSummary = TaskOutputSummary()
        if output is None:
            return task_result, output_files, summary

        task_result.outcome = "success" if output.success else "failure"
        task_result.reducer = output.reducer

        _process_function_output(
            function_output=output.function_output,
            output_files=output_files,
            summary=summary,
        )
        _process_router_output(
            router_output=output.router_output, task_result=task_result, summary=summary
        )
        _process_stdout(
            stdout=output.stdout, output_files=output_files, summary=summary
        )
        _process_stderr(
            stderr=output.stderr, output_files=output_files, summary=summary
        )

        summary.total_bytes = (
            summary.output_total_bytes
            + summary.stdout_total_bytes
            + summary.stderr_total_bytes
        )

        return task_result, output_files, summary


def _process_function_output(
    function_output: Optional[FunctionOutput],
    output_files: List[Any],
    summary: TaskOutputSummary,
) -> None:
    if function_output is None:
        return

    for output in function_output.outputs or []:
        payload = output.bytes if output.HasField("bytes") else output.string
        output_files.append(
            (
                "node_outputs",
                (nanoid.generate(), payload, output.content_type),
            )
        )
        summary.output_count += 1
        summary.output_total_bytes += len(payload)


def _process_router_output(
    router_output: Optional[RouterOutput],
    task_result: TaskResult,
    summary: TaskOutputSummary,
) -> None:
    if router_output is None:
        return

    task_result.router_output = RouterOutput(edges=router_output.edges)
    summary.router_output_count += 1


def _process_stdout(
    stdout: Optional[str], output_files: List[Any], summary: TaskOutputSummary
) -> None:
    if stdout is None:
        return

    output_files.append(
        (
            "stdout",
            (
                nanoid.generate(),
                stdout.encode(),
                UTF_8_CONTENT_TYPE,
            ),
        )
    )
    summary.stdout_count += 1
    summary.stdout_total_bytes += len(stdout)


def _process_stderr(
    stderr: Optional[str], output_files: List[Any], summary: TaskOutputSummary
) -> None:
    if stderr is None:
        return

    output_files.append(
        (
            "stderr",
            (
                nanoid.generate(),
                stderr.encode(),
                UTF_8_CONTENT_TYPE,
            ),
        )
    )
    summary.stderr_count += 1
    summary.stderr_total_bytes += len(stderr)
