import hashlib
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from indexify.proto.executor_api_pb2 import (
    DataPayload,
    DataPayloadEncoding,
    ReportTaskOutcomeRequest,
    TaskOutcome,
)
from indexify.proto.executor_api_pb2_grpc import ExecutorAPIStub

from .blob_store.blob_store import BLOBStore
from .function_executor.task_output import TaskOutput
from .grpc.channel_manager import ChannelManager
from .metrics.task_reporter import (
    metric_report_task_outcome_errors,
    metric_report_task_outcome_latency,
    metric_report_task_outcome_rpcs,
    metric_task_output_blob_store_upload_errors,
    metric_task_output_blob_store_upload_latency,
    metric_task_output_blob_store_uploads,
)


@dataclass
class UploadedTaskOutput:
    data_payloads: List[DataPayload]
    stdout: Optional[DataPayload]
    stderr: Optional[DataPayload]


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
        executor_id: str,
        channel_manager: ChannelManager,
        blob_store: BLOBStore,
    ):
        self._executor_id = executor_id
        self._is_shutdown = False
        self._channel_manager = channel_manager
        self._blob_store = blob_store

    async def shutdown(self) -> None:
        """Shuts down the task reporter.

        Task reporter stops reporting all task outcomes to the Server.
        There are many task failures due to Executor shutdown. We give wrong
        signals to Server if we report such failures.
        """
        self._is_shutdown = True

    async def report(self, output: TaskOutput, logger: Any) -> None:
        """Reports result of the supplied task."""
        logger = logger.bind(module=__name__)

        if self._is_shutdown:
            logger.warning(
                "task reporter got shutdown, skipping task outcome reporting"
            )
            return

        # TODO: If the files are uploaded successfully,
        # we should record that so that if we fail to report
        # the task outcome, we don't retry the upload.
        # This will save us some time and resources.
        # It's good to do this once we delete all the legacy code paths.

        output_summary: TaskOutputSummary = _task_output_summary(output)
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

        start_time = time.time()
        with (
            metric_task_output_blob_store_upload_latency.time(),
            metric_task_output_blob_store_upload_errors.count_exceptions(),
        ):
            metric_task_output_blob_store_uploads.inc()
            uploaded_task_output: UploadedTaskOutput = (
                await self._upload_output_to_blob_store(output, logger)
            )

        logger.info(
            "files uploaded to blob store",
            duration=time.time() - start_time,
        )

        request = ReportTaskOutcomeRequest(
            task_id=output.task_id,
            namespace=output.namespace,
            graph_name=output.graph_name,
            function_name=output.function_name,
            graph_invocation_id=output.graph_invocation_id,
            outcome=_to_grpc_task_outcome(output),
            executor_id=self._executor_id,
            reducer=output.reducer,
            next_functions=(output.router_output.edges if output.router_output else []),
            fn_outputs=uploaded_task_output.data_payloads,
            stdout=uploaded_task_output.stdout,
            stderr=uploaded_task_output.stderr,
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

    async def _upload_output_to_blob_store(
        self, output: TaskOutput, logger: Any
    ) -> UploadedTaskOutput:
        data_payloads: List[DataPayload] = []
        stdout: Optional[DataPayload] = None
        stderr: Optional[DataPayload] = None

        if output.stdout is not None:
            stdout_url = f"{output.output_payload_uri_prefix}.{output.task_id}.stdout"
            stdout_bytes: bytes = output.stdout.encode()
            await self._blob_store.put(stdout_url, stdout_bytes, logger)
            stdout = DataPayload(
                uri=stdout_url,
                size=len(stdout_bytes),
                sha256_hash=_compute_hash(stdout_bytes),
                encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
                encoding_version=0,
            )

        if output.stderr is not None:
            stderr_url = f"{output.output_payload_uri_prefix}.{output.task_id}.stderr"
            stderr_bytes: bytes = output.stderr.encode()
            await self._blob_store.put(stderr_url, stderr_bytes, logger)
            stderr = DataPayload(
                uri=stderr_url,
                size=len(stderr_bytes),
                sha256_hash=_compute_hash(stderr_bytes),
                encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
                encoding_version=0,
            )

        if output.function_output is not None:
            for func_output_item in output.function_output.outputs:
                node_output_sequence = len(data_payloads)
                if output.reducer:
                    # Reducer tasks have to write their results into the same blob.
                    output_url = (
                        f"{output.output_payload_uri_prefix}.{node_output_sequence}"
                    )
                else:
                    # Regular tasks write their results into different blobs made unique using task ids.
                    output_url = f"{output.output_payload_uri_prefix}.{output.task_id}.{node_output_sequence}"

                output_bytes: bytes = (
                    func_output_item.bytes
                    if func_output_item.HasField("bytes")
                    else func_output_item.string.encode()
                )
                await self._blob_store.put(output_url, output_bytes, logger)
                data_payloads.append(
                    DataPayload(
                        uri=output_url,
                        size=len(output_bytes),
                        sha256_hash=_compute_hash(output_bytes),
                        encoding=_to_grpc_data_payload_encoding(output),
                        encoding_version=0,
                    )
                )

        return UploadedTaskOutput(
            data_payloads=data_payloads,
            stdout=stdout,
            stderr=stderr,
        )


def _task_output_summary(output: TaskOutput) -> TaskOutputSummary:
    summary: TaskOutputSummary = TaskOutputSummary()

    if output.stdout is not None:
        summary.stdout_count += 1
        summary.stdout_total_bytes += len(output.stdout)

    if output.stderr is not None:
        summary.stderr_count += 1
        summary.stderr_total_bytes += len(output.stderr)

    if output.function_output is not None:
        for func_output_item in output.function_output.outputs:
            output_len: bytes = len(
                func_output_item.bytes
                if func_output_item.HasField("bytes")
                else func_output_item.string
            )
            summary.output_count += 1
            summary.output_total_bytes += output_len

    if output.router_output is not None:
        summary.router_output_count += 1

    summary.total_bytes = (
        summary.output_total_bytes
        + summary.stdout_total_bytes
        + summary.stderr_total_bytes
    )
    return summary


def _to_grpc_task_outcome(task_output: TaskOutput) -> TaskOutcome:
    if task_output.success:
        return TaskOutcome.TASK_OUTCOME_SUCCESS
    else:
        return TaskOutcome.TASK_OUTCOME_FAILURE


def _to_grpc_data_payload_encoding(task_output: TaskOutput) -> DataPayloadEncoding:
    if task_output.output_encoding == "json":
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    else:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE


def _compute_hash(data: bytes) -> str:
    hasher = hashlib.sha256(usedforsecurity=False)
    hasher.update(data)
    return hasher.hexdigest()
