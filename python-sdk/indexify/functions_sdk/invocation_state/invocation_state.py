from typing import Any, Optional


class InvocationState:
    """Abstract interface for Graph invocation state key-value API.

    The API allows to set and get key-value pairs from Indexify functions.
    The key-value pairs are scoped per Graph invocation.
    Each new invocation starts with an empty state (empty set of key-value pairs).
    A value can be any CloudPickleSerializer serializable object."""

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair.

        Raises Exception if an error occured."""
        raise NotImplementedError()

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Get a value by key. If the key does not exist, return the default value.

        Raises Exception if an error occured."""
        raise NotImplementedError()
