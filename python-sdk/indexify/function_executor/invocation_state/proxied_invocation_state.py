from typing import Any, Optional

from indexify.functions_sdk.invocation_state.invocation_state import (
    InvocationState,
)
from indexify.functions_sdk.object_serializer import CloudPickleSerializer

from .invocation_state_proxy_server import InvocationStateProxyServer


class ProxiedInvocationState(InvocationState):
    """InvocationState that proxies the calls via InvocationStateProxyServer."""

    def __init__(self, task_id: str, proxy_server: InvocationStateProxyServer):
        self._task_id: str = task_id
        self._proxy_server: InvocationStateProxyServer = proxy_server

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair."""
        self._proxy_server.set(
            self._task_id, key, CloudPickleSerializer.serialize(value)
        )

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Get a value by key. If the key does not exist, return the default value."""
        serialized_value: Optional[bytes] = self._proxy_server.get(self._task_id, key)
        return (
            default
            if serialized_value is None
            else CloudPickleSerializer.deserialize(serialized_value)
        )
