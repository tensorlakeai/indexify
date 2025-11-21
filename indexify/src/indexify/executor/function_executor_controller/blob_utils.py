from typing import Any

from tensorlake.function_executor.proto.function_executor_pb2 import (
    BLOB,
    BLOBChunk,
    SerializedObjectEncoding,
    SerializedObjectInsideBLOB,
    SerializedObjectManifest,
)

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.proto.executor_api_pb2 import (
    DataPayload,
    DataPayloadEncoding,
)

# The following constants are subject to S3 limits,
# see https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.
#
# 7 days - max presigned URL validity duration and limit on max function duration
_MAX_PRESIGNED_URI_EXPIRATION_SEC: int = 7 * 24 * 60 * 60
# This chunk size gives the best performance with S3. Based on our benchmarking.
_BLOB_OPTIMAL_CHUNK_SIZE_BYTES: int = 100 * 1024 * 1024  # 100 MB
# Max output size with optimal chunks is 100 * 100 MB = 10 GB.
# Each chunk requires a separate S3 presign operation, so we limit the number of optimal chunks to 100.
# S3 presign operations are local, it typically takes 30 ms per 100 URLs.
_OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: int = 100
# This chunk size gives ~20% slower performance with S3 compared to optimal. Based on our benchmarking.
_OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES: int = 1 * 1024 * 1024 * 1024  # 1 GB


async def presign_read_only_blob_for_data_payload(
    data_payload: DataPayload, blob_store: BLOBStore, logger: Any
) -> BLOB:
    return await presign_read_only_blob(
        blob_uri=data_payload.uri,
        blob_size=data_payload.offset + data_payload.size,
        blob_store=blob_store,
        logger=logger,
    )


async def presign_read_only_blob(
    blob_uri: str, blob_size: int, blob_store: BLOBStore, logger: Any
) -> BLOB:
    get_blob_uri: str = await blob_store.presign_get_uri(
        uri=blob_uri,
        expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
        logger=logger,
    )

    chunks: list[BLOBChunk] = []
    chunks_total_size: int = 0

    while chunks_total_size < blob_size:
        # Trim chunk size if it exceeds the remaining data size.
        # This is important because FE uses blob size derived from its chunk sizes.
        chunk_size: int = (
            _BLOB_OPTIMAL_CHUNK_SIZE_BYTES
            if chunks_total_size + _BLOB_OPTIMAL_CHUNK_SIZE_BYTES <= blob_size
            else blob_size - chunks_total_size
        )
        chunks_total_size += chunk_size
        chunks.append(
            BLOBChunk(
                uri=get_blob_uri,
                size=chunk_size,
                # ETag is only set by FE when returning BLOBs to us
            )
        )

    return BLOB(
        chunks=chunks,
    )


async def presign_write_only_blob(
    blob_id: str,
    blob_uri: str,
    upload_id: str,
    size: int,
    blob_store: BLOBStore,
    logger: Any,
) -> BLOB:
    """Presigns the output blob for the allocation."""
    chunks: list[BLOBChunk] = []
    chunks_total_size: int = 0

    while chunks_total_size < size:
        upload_chunk_uri: str = await blob_store.presign_upload_part_uri(
            uri=blob_uri,
            part_number=len(chunks) + 1,
            upload_id=upload_id,
            expires_in_sec=_MAX_PRESIGNED_URI_EXPIRATION_SEC,
            logger=logger,
        )

        # Determine optimal chunk size.
        chunk_size: int = (
            _BLOB_OPTIMAL_CHUNK_SIZE_BYTES
            if len(chunks) < _OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT
            else _OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES
        )
        # Trim chunk size if it exceeds the remaining data size.
        # This is important because FE uses blob size derived from its chunk sizes.
        chunk_size = (
            chunk_size
            if chunks_total_size + chunk_size <= size
            else size - chunks_total_size
        )
        chunks_total_size += chunk_size
        chunks.append(
            BLOBChunk(
                uri=upload_chunk_uri,
                size=chunk_size,
                # ETag is only set by FE when returning BLOBs to us
            )
        )

    return BLOB(
        id=blob_id,
        chunks=chunks,
    )


def data_payload_to_serialized_object_inside_blob(
    data_payload: DataPayload,
) -> SerializedObjectInsideBLOB:
    return SerializedObjectInsideBLOB(
        manifest=serialized_object_manifest_from_data_payload(data_payload),
        offset=data_payload.offset,
    )


def serialized_object_manifest_from_data_payload(
    data_payload: DataPayload,
) -> SerializedObjectManifest:
    """Converts the given data payload into SerializedObjectManifest accepted by Function Executor.

    Raises ValueError if the supplied data payload can't be converted.
    """
    so_manifest: SerializedObjectManifest = SerializedObjectManifest(
        # Server currently ignores encoding version so we set it to default 0.
        encoding_version=(
            data_payload.encoding_version
            if data_payload.HasField("encoding_version")
            else 0
        ),
        sha256_hash=data_payload.sha256_hash,
        size=data_payload.size,
        metadata_size=data_payload.metadata_size,
    )
    if data_payload.HasField("content_type"):
        so_manifest.content_type = data_payload.content_type
    if data_payload.HasField("source_function_call_id"):
        so_manifest.source_function_call_id = data_payload.source_function_call_id
    # data_payload.id is not used.

    if data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_ZIP:
        so_manifest.encoding = (
            SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP
        )
    elif data_payload.encoding == DataPayloadEncoding.DATA_PAYLOAD_ENCODING_RAW:
        so_manifest.encoding = SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW
    else:
        raise ValueError(
            f"Can't convert data payload {data_payload} into serialized object"
        )

    return so_manifest


def to_data_payload(
    so: SerializedObjectInsideBLOB,
    blob_uri: str,
    logger: Any,
) -> DataPayload:
    """Converts a serialized object inside BLOB into a DataPayload."""
    # TODO: Validate SerializedObjectInsideBLOB.
    return DataPayload(
        uri=blob_uri,
        encoding=_to_data_payload_encoding(so.manifest.encoding, logger),
        encoding_version=so.manifest.encoding_version,
        content_type=(
            so.manifest.content_type if so.manifest.HasField("content_type") else None
        ),
        metadata_size=so.manifest.metadata_size,
        offset=so.offset,
        size=so.manifest.size,
        sha256_hash=so.manifest.sha256_hash,
        source_function_call_id=so.manifest.source_function_call_id,
        # id is not used
    )


def _to_data_payload_encoding(
    encoding: SerializedObjectEncoding, logger: Any
) -> DataPayloadEncoding:
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_BINARY_PICKLE
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_JSON
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_TEXT:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UTF8_TEXT
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_RAW:
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_RAW
    else:
        logger.error(
            "unexpected encoding for SerializedObject",
            encoding=SerializedObjectEncoding.Name(encoding),
        )
        return DataPayloadEncoding.DATA_PAYLOAD_ENCODING_UNKNOWN
