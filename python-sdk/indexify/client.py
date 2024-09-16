from typing import Optional

from .settings import DEFAULT_SERVICE_URL
from .base_client import IndexifyClient
from .local_runner import LocalRunner
from .remote_client import RemoteClient


def create_client(
    service_url: str = DEFAULT_SERVICE_URL,
    config_path: Optional[str] = None,
    local: bool = False,
    *args,
    **kwargs,
) -> IndexifyClient:
    if local:
        return LocalRunner()
    return RemoteClient(config_path=config_path, service_url=service_url, **kwargs)
 