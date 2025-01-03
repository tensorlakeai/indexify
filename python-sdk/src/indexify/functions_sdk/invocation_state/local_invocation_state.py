from typing import Any, Dict, Optional

from ..object_serializer import CloudPickleSerializer
from .invocation_state import InvocationState


class LocalInvocationState(InvocationState):
    """InvocationState that stores the key-value pairs in memory.

    This is intended to be used with local graphs."""

    def __init__(self):
        """Creates a new instance.

        Caller needs to ensure that the returned instance is only used for a single invocation state.
        """
        self._state: Dict[str, bytes] = {}

    def set(self, key: str, value: Any) -> None:
        # It's important to serialize the value even in the local implementation
        # so there are no unexpected errors when running in remote graph mode.
        self._state[key] = CloudPickleSerializer.serialize(value)

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        serialized_value: Optional[bytes] = self._state.get(key, None)
        return (
            default
            if serialized_value is None
            else CloudPickleSerializer.deserialize(serialized_value)
        )
