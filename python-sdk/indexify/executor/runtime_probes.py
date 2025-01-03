import os
import platform
import sys
from typing import Any, Dict, Tuple

from pydantic import BaseModel

DEFAULT_EXECUTOR = "tensorlake/indexify-executor-default"
# Empty string is used as a default hash which tells the scheduler to accept any hash.
DEFAULT_HASH = ""


class ProbeInfo(BaseModel):
    image_name: str
    image_hash: str
    python_major_version: int
    labels: Dict[str, Any] = {}
    is_default_executor: bool


class RuntimeProbes:
    def __init__(self) -> None:
        self._image_name = self._read_image_name()
        self._image_hash = self._read_image_hash()
        self._os_name = platform.system()
        self._architecture = platform.machine()
        (
            self._python_version_major,
            self._python_version_minor,
        ) = self._get_python_version()

    def _read_image_name(self) -> str:
        file_path = os.path.expanduser("~/.indexify/image_name")
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                return file.read().strip()
        return DEFAULT_EXECUTOR

    def _read_image_hash(self) -> str:
        file_path = os.path.expanduser("~/.indexify/image_hash")
        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                return file.read().strip()
        return DEFAULT_HASH

    def _get_python_version(self) -> Tuple[int, int]:
        version_info = sys.version_info
        return version_info.major, version_info.minor

    def _is_default_executor(self):
        return True if self._read_image_name() == DEFAULT_EXECUTOR else False

    def probe(self) -> ProbeInfo:
        labels = {
            "os": self._os_name,
            "image_name": self._image_name,
            "architecture": self._architecture,
            "python_major_version": self._python_version_major,
            "python_minor_version": self._python_version_minor,
        }

        return ProbeInfo(
            image_name=self._image_name,
            image_hash=self._image_hash,
            python_major_version=self._python_version_major,
            labels=labels,
            is_default_executor=self._is_default_executor(),
        )
