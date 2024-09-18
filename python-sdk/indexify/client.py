from typing import Optional

from .base_client import IndexifyClient
from .local_client import LocalClient
from .remote_client import RemoteClient
from .settings import DEFAULT_SERVICE_URL


def create_client(
    service_url: str = DEFAULT_SERVICE_URL,
    config_path: Optional[str] = None,
    local: bool = False,
    *args,
    **kwargs,
) -> IndexifyClient:
    if local:
        return LocalClient()
    return RemoteClient(config_path=config_path, service_url=service_url, **kwargs)
