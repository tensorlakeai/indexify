import asyncio
from typing import List, Set


class CompletedTasksContainer:
    """An asyncio concurrent container for the completed task IDs."""

    def __init__(self):
        # The fields below are protected by the lock.
        self._lock: asyncio.Lock = asyncio.Lock()
        self._completed_task_ids: Set[str] = set()

    async def add(self, task_id: str) -> None:
        """Add a task to the container."""
        async with self._lock:
            self._completed_task_ids.add(task_id)

    async def contains(self, task_id: str) -> bool:
        """Check if the task is in the container."""
        async with self._lock:
            return task_id in self._completed_task_ids

    async def replace(self, task_ids: List[str]) -> None:
        """Replaces the task IDs with the supplied task IDs."""
        async with self._lock:
            self._completed_task_ids = set(task_ids)
