import asyncio
import hashlib
import time
from typing import Any, List

from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObject,
    SerializedObjectEncoding,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    DataPayload,
    DataPayloadEncoding,
)

from .events import TaskOutputUploadFinished
from .metrics.upload_task_output import (
    metric_task_output_blob_store_upload_errors,
    metric_task_output_blob_store_upload_latency,
    metric_task_output_blob_store_uploads,
    metric_task_output_upload_latency,
    metric_task_output_upload_retries,
    metric_task_output_uploads,
    metric_tasks_uploading_outputs,
)
from .task_info import TaskInfo
from .task_output import TaskOutput

_TASK_OUTPUT_UPLOAD_BACKOFF_SEC = 5.0


async def upload_task_output(
    task_info: TaskInfo, blob_store: BLOBStore, logger: Any
) -> TaskOutputUploadFinished:
    """Uploads the task output to blob store.

    Doesn't raise any Exceptions. Runs till the reporting is successful.
    """
    logger = logger.bind(module=__name__)

    with (
        metric_tasks_uploading_outputs.track_inprogress(),
        metric_task_output_upload_latency.time(),
    ):
        metric_task_output_uploads.inc()
        await _upload_task_output_until_successful(
            output=task_info.output,
            blob_store=blob_store,
            logger=logger,
        )
    _log_function_metrics(output=task_info.output, logger=logger)
    return TaskOutputUploadFinished(task_info=task_info, is_success=True)


async def _upload_task_output_until_successful(
    output: TaskOutput, blob_store: BLOBStore, logger: Any
) -> None:
    upload_retries: int = 0

    while True:
        logger = logger.bind(retries=upload_retries)
        try:
            await _upload_task_output_once(
                output=output, blob_store=blob_store, logger=logger
            )
            return
        except Exception as e:
            logger.error(
                "failed to upload task output",
                exc_info=e,
            )
            upload_retries += 1
            metric_task_output_upload_retries.inc()
            await asyncio.sleep(_TASK_OUTPUT_UPLOAD_BACKOFF_SEC)


class _TaskOutputSummary:
    def __init__(self):
        self.output_count: int = 0
        self.output_total_bytes: int = 0
        self.next_functions_count: int = 0
        self.stdout_count: int = 0
        self.stdout_total_bytes: int = 0
        self.stderr_count: int = 0
        self.stderr_total_bytes: int = 0
        self.invocation_error_output_count: int = 0
        self.invocation_error_output_total_bytes: int = 0
        self.total_bytes: int = 0


async def _upload_task_output_once(
    output: TaskOutput, blob_store: BLOBStore, logger: Any
) -> None:
    """Uploads the supplied task output to blob store.

    Raises an Exception if the upload fails.
    """
    output_summary: _TaskOutputSummary = _task_output_summary(output)
    logger.info(
        "uploading task output to blob store",
        total_bytes=output_summary.total_bytes,
        total_files=output_summary.output_count
        + output_summary.stdout_count
        + output_summary.stderr_count
        + output_summary.invocation_error_output_count,
        output_files=output_summary.output_count,
        output_bytes=output_summary.total_bytes,
        next_functions_count=output_summary.next_functions_count,
        stdout_bytes=output_summary.stdout_total_bytes,
        stderr_bytes=output_summary.stderr_total_bytes,
        invocation_error_output_bytes=output_summary.invocation_error_output_total_bytes,
    )

    start_time = time.time()
    with (
        metric_task_output_blob_store_upload_latency.time(),
        metric_task_output_blob_store_upload_errors.count_exceptions(),
    ):
        metric_task_output_blob_store_uploads.inc()
        await _upload_to_blob_store(
            task_output=output, blob_store=blob_store, logger=logger
        )

    logger.info(
        "files uploaded to blob store",
        duration=time.time() - start_time,
    )


async def _upload_to_blob_store(
    task_output: TaskOutput, blob_store: BLOBStore, logger: Any
) -> None:
    if task_output.stdout is not None:
        stdout_url = f"{task_output.allocation.task.output_payload_uri_prefix}.{task_output.allocation.task.id}.stdout"
        stdout_bytes: bytes = task_output.stdout.encode()
        await blob_store.put(stdout_url, stdout_bytes, logger)
        task_output.uploaded_stdout = DataPayload(
            uri=stdout_url,
            size=len(stdout_bytes),
            sha256_hash=compute_hash(stdout_bytes),
            encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
            encoding_version=0,
        )
        # stdout is uploaded, free the memory used for it and don't upload again if we retry overall output upload again.
        task_output.stdout = None

    if task_output.stderr is not None:
        stderr_url = f"{task_output.allocation.task.output_payload_uri_prefix}.{task_output.allocation.task.id}.stderr"
        stderr_bytes: bytes = task_output.stderr.encode()
        await blob_store.put(stderr_url, stderr_bytes, logger)
        task_output.uploaded_stderr = DataPayload(
            uri=stderr_url,
            size=len(stderr_bytes),
            sha256_hash=compute_hash(stderr_bytes),
            encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
            encoding_version=0,
        )
        # stderr is uploaded, free the memory used for it and don't upload again if we retry overall output upload again.
        task_output.stderr = None

    if task_output.invocation_error_output is not None:
        invocation_error_output_url = (
            f"{task_output.allocation.task.output_payload_uri_prefix}.inverr."
            f"{task_output.allocation.task.graph_invocation_id}"
        )
        invocation_error_output_bytes: bytes = task_output.invocation_error_output.data
        await blob_store.put(
            invocation_error_output_url, invocation_error_output_bytes, logger
        )
        task_output.uploaded_invocation_error_output = DataPayload(
            uri=invocation_error_output_url,
            size=len(invocation_error_output_bytes),
            sha256_hash=compute_hash(invocation_error_output_bytes),
            encoding=_to_grpc_data_payload_encoding(
                task_output.invocation_error_output.encoding, logger
            ),
            encoding_version=0,
        )
        # Invocation error output is uploaded, free the memory used for it and don't upload again if we retry overall output upload again.
        task_output.invocation_error_output = None

    # We can't use the default empty list output.uploaded_data_payloads because it's a singleton.
    uploaded_data_payloads: List[DataPayload] = []
    for output in task_output.function_outputs:
        output: SerializedObject
        output_ix: int = len(uploaded_data_payloads)
        output_url: str = (
            f"{task_output.allocation.task.output_payload_uri_prefix}.{task_output.allocation.task.id}.{output_ix}"
        )
        await blob_store.put(output_url, output.data, logger)
        uploaded_data_payloads.append(
            DataPayload(
                uri=output_url,
                size=len(output.data),
                sha256_hash=compute_hash(output.data),
                encoding=_to_grpc_data_payload_encoding(output.encoding, logger),
                encoding_version=0,
            )
        )

    task_output.uploaded_data_payloads = uploaded_data_payloads
    # The output is uploaded, free the memory used for it and don't upload again if we retry overall output upload again.
    task_output.function_outputs = []


def _task_output_summary(task_output: TaskOutput) -> _TaskOutputSummary:
    summary: _TaskOutputSummary = _TaskOutputSummary()

    if task_output.stdout is not None:
        summary.stdout_count += 1
        summary.stdout_total_bytes += len(task_output.stdout)

    if task_output.stderr is not None:
        summary.stderr_count += 1
        summary.stderr_total_bytes += len(task_output.stderr)

    if task_output.invocation_error_output is not None:
        summary.invocation_error_output_count += 1
        summary.invocation_error_output_total_bytes += len(
            task_output.invocation_error_output.data
        )

    for output in task_output.function_outputs:
        output: SerializedObject
        output_len: bytes = len(output.data)
        summary.output_count += 1
        summary.output_total_bytes += output_len

    summary.next_functions_count = len(task_output.next_functions)

    summary.total_bytes = (
        summary.output_total_bytes
        + summary.stdout_total_bytes
        + summary.stderr_total_bytes
    )
    return summary


def _to_grpc_data_payload_encoding(
    encoding: SerializedObjectEncoding, logger: Any
) -> DataPayloadEncoding:
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT
    else:
        logger.error(
            "Unexpected encoding for SerializedObject",
            encoding=SerializedObjectEncoding.Name(encoding),
        )
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UNKNOWN


def compute_hash(data: bytes) -> str:
    hasher = hashlib.sha256(usedforsecurity=False)
    hasher.update(data)
    return hasher.hexdigest()


# Temporary workaround is logging customer metrics until we store them somewhere
# for future retrieval and processing.
def _log_function_metrics(output: TaskOutput, logger: Any):
    if output.metrics is None:
        return

    for counter_name, counter_value in output.metrics.counters.items():
        logger.info(
            "function_metric", counter_name=counter_name, counter_value=counter_value
        )
    for timer_name, timer_value in output.metrics.timers.items():
        logger.info("function_metric", timer_name=timer_name, timer_value=timer_value)
