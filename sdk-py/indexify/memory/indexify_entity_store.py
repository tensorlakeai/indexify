from typing import Any, Optional
from uuid import UUID

from langchain.memory.entity import BaseEntityStore

from indexify.indexify import Indexify, MemoryResult


class IndexifyEntityStore(BaseEntityStore):
    """Indexify Entity store"""

    session_id: str = "default"
    db_url: str = "sqlite://memory.db"

    def __init__(
        self,
        session_id: Optional[UUID],
        window_size: Optional[int],
        capacity: Optional[int],
        db_url: str = "sqlite://memory.db",
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.session_id = Indexify.create_memory_session(
            session_id, db_url, window_size, capacity
        )

    @property
    def session_id(self) -> str:
        return self.session_id

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        result: MemoryResult = Indexify.retrieve_records(self.session_id, key)
        return result or default

    def set(self, key: str, value: Optional[str]) -> None:
        Indexify.add_to_memory(self.session_id, key, value)

    def delete(self, _key: str) -> None:
        """unsupported"""
        pass

    def exists(self, _key: str) -> bool:
        """unsupported"""
        pass

    def clear(self) -> None:
        """unsupported"""
        pass
