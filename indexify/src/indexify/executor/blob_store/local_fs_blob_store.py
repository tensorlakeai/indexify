import asyncio
import os
import os.path
from typing import Any


class LocalFSBLOBStore:
    """BLOB store that stores BLOBs in local file system."""

    async def get(self, uri: str, logger: Any) -> bytes:
        """Returns binary value stored in file at the supplied URI.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Raises Exception on error. Raises KeyError if the file doesn't exist.
        """
        # Run synchronous code in a thread to not block the event loop.
        return await asyncio.to_thread(self._sync_get, _path_from_file_uri(uri))

    async def presign_get_uri(self, uri: str, expires_in_sec: int, logger: Any) -> str:
        """Returns a presigned URI for getting the file at the supplied URI.

        For local files, just returns the file URI itself.
        """
        return uri

    async def upload(self, uri: str, value: bytes, logger: Any) -> None:
        """Stores the supplied binary value in a file at the supplied URI.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Overwrites existing file. Raises Exception on error.
        """
        # Run synchronous code in a thread to not block the event loop.
        return await asyncio.to_thread(self._sync_put, _path_from_file_uri(uri), value)

    async def create_multipart_upload(self, uri: str, logger: Any) -> str:
        """Creates a multipart upload for local file and returns a dummy upload ID."""
        # Local files do not require multipart upload, return a dummy ID
        return "local-multipart-upload-id"

    async def complete_multipart_upload(
        self, uri: str, upload_id: str, parts_etags: list[str], logger: Any
    ) -> None:
        """Completes a multipart upload for local file. No-op for local files."""
        # No action needed for local files
        return None

    async def abort_multipart_upload(
        self, uri: str, upload_id: str, logger: Any
    ) -> None:
        """Aborts a multipart upload for local file. No-op for local files."""
        # No action needed for local files
        return None

    async def presign_upload_part_uri(
        self,
        uri: str,
        part_number: int,
        upload_id: str,
        expires_in_sec: int,
        logger: Any,
    ) -> str:
        """Returns a presigned URI for uploading a part in a multipart upload for local file.

        For local files, just returns the file URI itself.
        """
        return uri

    def _sync_get(self, path: str) -> bytes:
        if not os.path.isabs(path):
            raise ValueError(f"Path {path} must be absolute")

        if os.path.exists(path):
            with open(path, mode="rb") as blob_file:
                return blob_file.read()
        else:
            raise KeyError(f"File at {path} does not exist")

    def _sync_put(self, path: str, value: bytes) -> None:
        if not os.path.isabs(path):
            raise ValueError(f"Path {path} must be absolute")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode="wb") as blob_file:
            blob_file.write(value)


def _path_from_file_uri(uri: str) -> str:
    return uri[7:]  # strip "file://" prefix
