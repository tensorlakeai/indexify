import asyncio
import time
from typing import Any, List, Optional, Tuple

import nanoid
from httpx import Timeout
from tensorlake.function_executor.proto.function_executor_pb2 import FunctionOutput
from tensorlake.utils.http_client import get_httpx_client

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    OutputEncoding,
    ReportTaskOutcomeRequest,
    TaskOutcome,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from .api_objects import (
    TASK_OUTCOME_FAILURE,
    TASK_OUTCOME_SUCCESS,
    IngestFnOutputsResponse,
    RouterOutput,
    TaskResult,
)
from .function_executor.task_output import TaskOutput
from .grpc.channel_manager import ChannelManager
from .metrics.task_reporter import (
    metric_report_task_outcome_errors,
    metric_report_task_outcome_latency,
    metric_report_task_outcome_rpcs,
    metric_server_ingest_files_errors,
    metric_server_ingest_files_latency,
    metric_server_ingest_files_requests,
)


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
        self,
        base_url: str,
        executor_id: str,
        channel_manager: ChannelManager,
        config_path: Optional[str] = None,
    ):
        self._base_url = base_url
        self._executor_id = executor_id
        self._is_shutdown = False
        # Use thread-safe sync client due to issues with async client.
        # Async client attempts to use connections it already closed.
        # See e.g. https://github.com/encode/httpx/issues/2337.
        # Creating a new async client for each request fixes this but it
        # results in not reusing established TCP connections to server.
        self._client = get_httpx_client(config_path, make_async=False)
        self._channel_manager = channel_manager

    async def shutdown(self):
        """Shuts down the task reporter.

        Task reporter stops reporting all task outcomes to the Server.
        There are many task failures due to Executor shutdown. We give wrong
        signals to Server if we report such failures.
        """
        self._is_shutdown = True

    async def report(self, output: TaskOutput, logger: Any):
        """Reports result of the supplied task."""
        logger = logger.bind(module=__name__)

        if self._is_shutdown:
            logger.warning(
                "task reporter got shutdown, skipping task outcome reporting"
            )
            return

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

        start_time = time.time()
        with metric_server_ingest_files_latency.time():
            metric_server_ingest_files_requests.inc()
            # Run in a separate thread to not block the main event loop.
            response = await asyncio.to_thread(
                self._client.post,
                url=f"{self._base_url}/internal/ingest_fn_outputs",
                **kwargs,
            )
        end_time = time.time()
        logger.info(
            "files uploaded",
            response_time=end_time - start_time,
            response_code=response.status_code,
        )

        try:
            response.raise_for_status()
        except Exception as e:
            metric_server_ingest_files_errors.inc()
            # Caller catches and logs the exception.
            raise Exception(
                "failed to upload files. "
                f"Response code: {response.status_code}. "
                f"Response text: '{response.text}'."
            ) from e

        # TODO: If the files are uploaded successfully,
        # we should record that so that if we fail to report
        # the task outcome, we don't retry the upload.
        # This will save us some time and resources.

        ingested_files_response = response.json()
        ingested_files = IngestFnOutputsResponse.model_validate(ingested_files_response)
        fn_outputs = []
        for data_payload in ingested_files.data_payloads:
            fn_outputs.append(
                DataPayload(
                    path=data_payload.path,
                    size=data_payload.size,
                    sha256_hash=data_payload.sha256_hash,
                )
            )
        stdout, stderr = None, None
        if ingested_files.stdout:
            stdout = DataPayload(
                path=ingested_files.stdout.path,
                size=ingested_files.stdout.size,
                sha256_hash=ingested_files.stdout.sha256_hash,
            )
        if ingested_files.stderr:
            stderr = DataPayload(
                path=ingested_files.stderr.path,
                size=ingested_files.stderr.size,
                sha256_hash=ingested_files.stderr.sha256_hash,
            )

        request = ReportTaskOutcomeRequest(
            task_id=output.task_id,
            namespace=output.namespace,
            graph_name=output.graph_name,
            function_name=output.function_name,
            graph_invocation_id=output.graph_invocation_id,
            outcome=_to_grpc_task_outcome(output),
            invocation_id=output.graph_invocation_id,
            executor_id=self._executor_id,
            reducer=output.reducer,
            next_functions=(output.router_output.edges if output.router_output else []),
            fn_outputs=fn_outputs,
            stdout=stdout,
            stderr=stderr,
            output_encoding=_to_grpc_output_encoding(output),
            output_encoding_version=0,
        )
        try:
            stub = ExecutorAPIStub(await self._channel_manager.get_channel())
            with (
                metric_report_task_outcome_latency.time(),
                metric_report_task_outcome_errors.count_exceptions(),
            ):
                metric_report_task_outcome_rpcs.inc()
                await stub.report_task_outcome(request, timeout=5.0)
        except Exception as e:
            logger.error("failed to report task outcome", error=e)
            raise e

    def _process_task_output(
        self, output: TaskOutput
    ) -> Tuple[TaskResult, List[Any], TaskOutputSummary]:
        task_result = TaskResult(
            outcome="failure",
            namespace=output.namespace,
            compute_graph=output.graph_name,
            compute_fn=output.function_name,
            invocation_id=output.graph_invocation_id,
            executor_id=self._executor_id,
            task_id=output.task_id,
        )
        output_files: List[Any] = []
        summary: TaskOutputSummary = TaskOutputSummary()
        if output is None:
            return task_result, output_files, summary

        task_result.outcome = (
            TASK_OUTCOME_SUCCESS if output.success else TASK_OUTCOME_FAILURE
        )
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


def _to_grpc_task_outcome(task_output: TaskOutput) -> TaskOutcome:
    if task_output.success:
        return TaskOutcome.TASK_OUTCOME_SUCCESS
    else:
        return TaskOutcome.TASK_OUTCOME_FAILURE


def _to_grpc_output_encoding(task_output: TaskOutput) -> OutputEncoding:
    if task_output.output_encoding == "json":
        return OutputEncoding.OUTPUT_ENCODING_JSON
    else:
        return OutputEncoding.OUTPUT_ENCODING_PICKLE
