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

    async def put(self, uri: str, value: bytes, logger: Any) -> None:
        """Stores the supplied binary value in a file at the supplied URI.

        The URI must be a file URI (starts with "file://"). The path must be absolute.
        Overwrites existing file. Raises Exception on error.
        """
        # Run synchronous code in a thread to not block the event loop.
        return await asyncio.to_thread(self._sync_put, _path_from_file_uri(uri), value)

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
