import os
import platform
import sys
from typing import Any, Dict, Optional, Tuple

import psutil
from pydantic import BaseModel

class GPUResources(BaseModel):
    gpu_model: str
    num_cards: int
    memory: int

class ProbeInfo(BaseModel):
    python_major_version: int
    labels: Dict[str, Any] = {}
    memory: int
    gpu_resources: Optional[GPUResources] = None

class RuntimeProbes:
    def __init__(self) -> None:
        self._os_name = platform.system()
        self._architecture = platform.machine()
        (
            self._python_version_major,
            self._python_version_minor,
        ) = self._get_python_version()
        self._memory = psutil.virtual_memory().total
        try:
            from pynvml import nvmlInit, nvmlDeviceGetHandleByIndex, nvmlDeviceGetName, nvmlDeviceGetCount, nvmlDeviceGetMemoryInfo
            nvmlInit()
            deviceCount = nvmlDeviceGetCount()
            if deviceCount > 0:
                handle = nvmlDeviceGetHandleByIndex(0)
                name = nvmlDeviceGetName(handle)
                memory = nvmlDeviceGetMemoryInfo(handle).total
                self._gpu_resources = GPUResources(gpu_model=name, num_cards=deviceCount, memory=memory)
        except Exception as e:
            self._gpu_resources = None

    def _get_python_version(self) -> Tuple[int, int]:
        version_info = sys.version_info
        return version_info.major, version_info.minor

    def probe(self) -> ProbeInfo:
        labels = {
            "os": self._os_name,
            "architecture": self._architecture,
            "python_runtime": f"{self._python_version_major}.{self._python_version_minor}",
            "python_major_version": self._python_version_major,
            "python_minor_version": self._python_version_minor,
        }

        return ProbeInfo(
            python_major_version=self._python_version_major,
            labels=labels,
            memory=self._memory,
            gpu_resources=self._gpu_resources,
        )
