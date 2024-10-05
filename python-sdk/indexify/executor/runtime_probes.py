import os
import platform
import sys
from typing import Any, Dict, Tuple

from pydantic import BaseModel


class ProbeInfo(BaseModel):
    image_name: str
    python_major_version: int
    labels: Dict[str, Any] = {}


class RuntimeProbes:
    def __init__(self) -> None:
        self._image_name = self._read_image_name()
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
        return "tensorlake/indexify-executor-default"

    def _get_python_version(self) -> Tuple[int, int]:
        version_info = sys.version_info
        return version_info.major, version_info.minor

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
            python_major_version=self._python_version_major,
            labels=labels,
        )
