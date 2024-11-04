import asyncio
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel
from rich import print

from indexify.functions_sdk.data_objects import IndexifyData, RouterOutput

from .api_objects import Task


class CompletedTask(BaseModel):
    task: Task
    task_outcome: Literal["success", "failure"]
    outputs: Optional[List[IndexifyData]] = None
    router_output: Optional[RouterOutput] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    reducer: bool = False


class TaskStore:
    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
        self._running_tasks: Dict[str, Task] = {}
        self._finished: Dict[str, CompletedTask] = {}
        self._retries: Dict[str, int] = {}
        self._new_task_event = asyncio.Event()
        self._finished_task_event = asyncio.Event()

    def get_task(self, id) -> Task:
        return self._tasks[id]

    def add_tasks(self, tasks: List[Task]):
        task: Task
        for task in tasks:
            if (
                (task.id in self._tasks)
                or (task.id in self._running_tasks)
                or (task.id in self._finished)
            ):
                continue
            print(
                f"[bold] task store: [/bold] added task: {task.id} graph: {task.compute_graph} fn: {task.compute_fn} to queue"
            )
            self._tasks[task.id] = task
            self._new_task_event.set()

    async def get_runnable_tasks(self) -> Dict[str, Task]:
        while True:
            runnable_tasks = set(self._tasks) - set(self._running_tasks)
            runnable_tasks = set(runnable_tasks) - set(self._finished)
            if len(runnable_tasks) == 0:
                await self._new_task_event.wait()
                self._new_task_event.clear()
            else:
                break
        out = {}
        for task_id in runnable_tasks:
            out[task_id] = self._tasks[task_id]
            self._running_tasks[task_id] = self._tasks[task_id]
        return out

    def complete(self, outcome: CompletedTask):
        self._retries.pop(outcome.task.id, None)
        self._finished[outcome.task.id] = outcome
        if outcome.task.id in self._running_tasks:
            self._running_tasks.pop(outcome.task.id)
        self._finished_task_event.set()

    def retriable_failure(self, task_id: str):
        self._running_tasks.pop(task_id)
        if task_id not in self._retries:
            self._retries[task_id] = 0
        self._retries[task_id] += 1
        if self._retries[task_id] > 3:
            self._retries.pop(task_id)
            self.complete(
                outcome=CompletedTask(
                    task_id=task_id, task_outcome="failed", outputs=[]
                )
            )
        else:
            self._new_task_event.set()

    def mark_reported(self, task_id: str):
        self._tasks.pop(task_id)
        self._finished.pop(task_id)
        print(f"[bold] task store: [/bold] removed task: {task_id} from queue")

    def report_failed(self, task_id: str):
        if self._finished[task_id].task_outcome != "Failed":
            # An error occurred while reporting the task, mark it as failed
            # and try reporting again.
            self._finished[task_id].task_outcome = "Failed"
        else:
            # If a task is already marked as failed, remove it from the queue.
            # The only possible error at this point is task not present at
            # the coordinator.
            self._tasks.pop(task_id)

    def num_pending_tasks(self) -> int:
        return len(self._tasks) + len(self._running_tasks)

    async def task_outcomes(self) -> List[CompletedTask]:
        while True:
            if len(self._finished) == 0:
                await self._finished_task_event.wait()
                self._finished_task_event.clear()
            else:
                break
        return self._finished.copy().values()
