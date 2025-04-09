from typing import Any, Optional

from .local_fs_blob_store import LocalFSBLOBStore
from .metrics.blob_store import (
    metric_get_blob_errors,
    metric_get_blob_latency,
    metric_get_blob_requests,
    metric_put_blob_errors,
    metric_put_blob_latency,
    metric_put_blob_requests,
)
from .s3_blob_store import S3BLOBStore


class BLOBStore:
    """Dispatches generic BLOB store calls to their real backends."""

    def __init__(
        self, local: Optional[LocalFSBLOBStore] = None, s3: Optional[S3BLOBStore] = None
    ):
        """Creates a BLOB store that uses the supplied BLOB stores."""
        self._local: Optional[LocalFSBLOBStore] = local
        self._s3: Optional[S3BLOBStore] = s3

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
                self._check_local_is_available()
                return await self._local.get(uri, logger)
            else:
                self._check_s3_is_available()
                return await self._s3.get(uri, logger)

    async def put(self, uri: str, value: bytes, logger: Any) -> None:
        """Stores the supplied binary value in a BLOB with the supplied URI.

        Overwrites existing BLOB. Raises Exception on error.
        """
        with (
            metric_put_blob_errors.count_exceptions(),
            metric_put_blob_latency.time(),
        ):
            metric_put_blob_requests.inc()
            if _is_file_uri(uri):
                self._check_local_is_available()
                await self._local.put(uri, value, logger)
            else:
                self._check_s3_is_available()
                await self._s3.put(uri, value, logger)

    def _check_local_is_available(self):
        if self._local is None:
            raise RuntimeError("Local file system BLOB store is not available")

    def _check_s3_is_available(self):
        if self._s3 is None:
            raise RuntimeError("S3 BLOB store is not available")


def _is_file_uri(uri: str) -> bool:
    return uri.startswith("file://")
