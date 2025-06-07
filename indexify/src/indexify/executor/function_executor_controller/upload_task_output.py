import asyncio
import hashlib
import time
from typing import Any

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
        self.router_output_count: int = 0
        self.stdout_count: int = 0
        self.stdout_total_bytes: int = 0
        self.stderr_count: int = 0
        self.stderr_total_bytes: int = 0
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
        await _upload_to_blob_store(output=output, blob_store=blob_store, logger=logger)

    logger.info(
        "files uploaded to blob store",
        duration=time.time() - start_time,
    )


async def _upload_to_blob_store(
    output: TaskOutput, blob_store: BLOBStore, logger: Any
) -> None:
    if output.stdout is not None:
        stdout_url = f"{output.task.output_payload_uri_prefix}.{output.task.id}.stdout"
        stdout_bytes: bytes = output.stdout.encode()
        await blob_store.put(stdout_url, stdout_bytes, logger)
        output.uploaded_stdout = DataPayload(
            uri=stdout_url,
            size=len(stdout_bytes),
            sha256_hash=_compute_hash(stdout_bytes),
            encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
            encoding_version=0,
        )
        # stdout is uploaded, free the memory used for it.
        output.stdout = None

    if output.stderr is not None:
        stderr_url = f"{output.task.output_payload_uri_prefix}.{output.task.id}.stderr"
        stderr_bytes: bytes = output.stderr.encode()
        await blob_store.put(stderr_url, stderr_bytes, logger)
        output.uploaded_stderr = DataPayload(
            uri=stderr_url,
            size=len(stderr_bytes),
            sha256_hash=_compute_hash(stderr_bytes),
            encoding=DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT,
            encoding_version=0,
        )
        # stderr is uploaded, free the memory used for it.
        output.stderr = None

    if output.function_output is not None:
        # We can't use the default empty list output.uploaded_data_payloads because it's a singleton.
        uploaded_data_payloads = []
        for func_output_item in output.function_output.outputs:
            node_output_sequence = len(uploaded_data_payloads)
            output_url = f"{output.task.output_payload_uri_prefix}.{output.task.id}.{node_output_sequence}"
            output_bytes: bytes = (
                func_output_item.bytes
                if func_output_item.HasField("bytes")
                else func_output_item.string.encode()
            )
            await blob_store.put(output_url, output_bytes, logger)
            uploaded_data_payloads.append(
                DataPayload(
                    uri=output_url,
                    size=len(output_bytes),
                    sha256_hash=_compute_hash(output_bytes),
                    encoding=_to_grpc_data_payload_encoding(output),
                    encoding_version=0,
                )
            )

        output.uploaded_data_payloads = uploaded_data_payloads
        # The output is uploaded, free the memory used for it.
        output.function_output = None


def _task_output_summary(output: TaskOutput) -> _TaskOutputSummary:
    summary: _TaskOutputSummary = _TaskOutputSummary()

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


def _to_grpc_data_payload_encoding(task_output: TaskOutput) -> DataPayloadEncoding:
    if task_output.output_encoding == "json":
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    else:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE


def _compute_hash(data: bytes) -> str:
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
