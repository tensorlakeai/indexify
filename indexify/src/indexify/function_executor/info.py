import importlib.metadata
import sys
from typing import Any, Dict


def info_response_kv_args() -> Dict[str, Any]:
    sdk_version = importlib.metadata.version("tensorlake")
    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    return {
        "version": "0.1.0",
        "sdk_version": sdk_version,
        "sdk_language": "python",
        "sdk_language_version": python_version,
    }
