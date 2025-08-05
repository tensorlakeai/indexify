from typing import Any

from .local_fs_blob_store import LocalFSBLOBStore
from .metrics.blob_store import (
    metric_abort_multipart_upload_errors,
    metric_abort_multipart_upload_latency,
    metric_abort_multipart_upload_requests,
    metric_complete_multipart_upload_errors,
    metric_complete_multipart_upload_latency,
    metric_complete_multipart_upload_requests,
    metric_create_multipart_upload_errors,
    metric_create_multipart_upload_latency,
    metric_create_multipart_upload_requests,
    metric_get_blob_errors,
    metric_get_blob_latency,
    metric_get_blob_requests,
    metric_presign_uri_errors,
    metric_presign_uri_latency,
    metric_presign_uri_requests,
    metric_upload_blob_errors,
    metric_upload_blob_latency,
    metric_upload_blob_requests,
)
from .s3_blob_store import S3BLOBStore


class BLOBStore:
    """Dispatches generic BLOB store calls to their real backends."""

    def __init__(self):
        self._local: LocalFSBLOBStore = LocalFSBLOBStore()
        self._s3: S3BLOBStore = S3BLOBStore()

    async def get(self, uri: str, logger: Any) -> bytes:
        """Returns binary value stored in BLOB with the supplied URI.

        Raises Exception on error. Raises KeyError if the BLOB doesn't exist.
        """
        with (
            metric_get_blob_errors.count_exceptions(),
            metric_get_blob_latency.time(),
        ):
            metric_get_blob_requests.inc()
            if _is_file_uri(uri):
                return await self._local.get(uri, logger)
            else:
                return await self._s3.get(uri, logger)

    async def presign_get_uri(self, uri: str, expires_in_sec: int, logger: Any) -> str:
        """Returns a presigned URI for getting the BLOB with the supplied URI.

        The URI allows to read any byte range in the BLOB."""
        with (
            metric_presign_uri_errors.count_exceptions(),
            metric_presign_uri_latency.time(),
        ):
            metric_presign_uri_requests.inc()
            if _is_file_uri(uri):
                return await self._local.presign_get_uri(uri, expires_in_sec, logger)
            else:
                return await self._s3.presign_get_uri(uri, expires_in_sec, logger)

    async def upload(self, uri: str, value: bytes, logger: Any) -> None:
        """Stores the supplied binary value in a BLOB with the supplied URI.

        Overwrites existing BLOB. Raises Exception on error.
        """
        with (
            metric_upload_blob_errors.count_exceptions(),
            metric_upload_blob_latency.time(),
        ):
            metric_upload_blob_requests.inc()
            if _is_file_uri(uri):
                await self._local.upload(uri, value, logger)
            else:
                await self._s3.upload(uri, value, logger)

    async def create_multipart_upload(self, uri: str, logger: Any) -> str:
        """Creates a multipart upload for BLOB with the supplied URI and returns the upload ID."""
        with (
            metric_create_multipart_upload_errors.count_exceptions(),
            metric_create_multipart_upload_latency.time(),
        ):
            metric_create_multipart_upload_requests.inc()
            if _is_file_uri(uri):
                return await self._local.create_multipart_upload(uri, logger)
            else:
                return await self._s3.create_multipart_upload(uri, logger)

    async def complete_multipart_upload(
        self, uri: str, upload_id: str, parts_etags: list[str], logger: Any
    ) -> None:
        """Completes a multipart upload for BLOB with the supplied URI.

        parts_etags is a list of ETags for the parts that were uploaded.
        The list is ordered by part number starting from 1.
        """
        with (
            metric_complete_multipart_upload_errors.count_exceptions(),
            metric_complete_multipart_upload_latency.time(),
        ):
            metric_complete_multipart_upload_requests.inc()
            if _is_file_uri(uri):
                await self._local.complete_multipart_upload(
                    uri, upload_id, parts_etags, logger
                )
            else:
                await self._s3.complete_multipart_upload(
                    uri, upload_id, parts_etags, logger
                )

    async def abort_multipart_upload(
        self, uri: str, upload_id: str, logger: Any
    ) -> None:
        """Aborts a multipart upload for BLOB with the supplied URI."""
        with (
            metric_abort_multipart_upload_errors.count_exceptions(),
            metric_abort_multipart_upload_latency.time(),
        ):
            metric_abort_multipart_upload_requests.inc()
            if _is_file_uri(uri):
                await self._local.abort_multipart_upload(uri, upload_id, logger)
            else:
                await self._s3.abort_multipart_upload(uri, upload_id, logger)

    async def presign_upload_part_uri(
        self,
        uri: str,
        part_number: int,
        upload_id: str,
        expires_in_sec: int,
        logger: Any,
    ) -> str:
        """Returns a presigned URI for uploading a part in a multipart upload.

        part_number starts from 1."""
        with (
            metric_presign_uri_errors.count_exceptions(),
            metric_presign_uri_latency.time(),
        ):
            metric_presign_uri_requests.inc()
            if _is_file_uri(uri):
                return await self._local.presign_upload_part_uri(
                    uri, part_number, upload_id, expires_in_sec, logger
                )
            else:
                return await self._s3.presign_upload_part_uri(
                    uri, part_number, upload_id, expires_in_sec, logger
                )


def _is_file_uri(uri: str) -> bool:
    return uri.startswith("file://")
