from typing import Any, Dict, Optional

from .api_objects import Task
from .function_executor.function_executor_state import FunctionExecutorState
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .function_executor.single_task_runner import SingleTaskRunner
from .function_executor.task_input import TaskInput
from .function_executor.task_output import TaskOutput


class TaskRunner:
    """Routes a task to its container following a scheduling policy.

    Due to the scheduling policy a task might be blocked for a while."""

    def __init__(
        self,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        config_path: Optional[str],
    ):
        self._factory: FunctionExecutorServerFactory = function_executor_server_factory
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        # We don't lock this map cause we never await while reading and modifying it.
        self._function_executor_states: Dict[str, FunctionExecutorState] = {}

    async def run(self, task_input: TaskInput, logger: Any) -> TaskOutput:
        logger = logger.bind(module=__name__)
        try:
            return await self._run(task_input, logger)
        except Exception as e:
            logger.error(
                "failed running the task",
                exc_info=e,
            )
            return TaskOutput.internal_error(task_input.task)

    async def _run(self, task_input: TaskInput, logger: Any) -> TaskOutput:
        state = self._get_or_create_state(task_input.task)
        async with state.lock:
            await self._run_task_policy(state, task_input.task)
            return await self._run_task(state, task_input, logger)

    async def _run_task_policy(self, state: FunctionExecutorState, task: Task) -> None:
        # Current policy for running tasks:
        #   - There can only be a single Function Executor per function regardless of function versions.
        #   --  If a Function Executor already exists for a different function version then wait until
        #       all the tasks finish in the existing Function Executor and then destroy it.
        #   --  This prevents failed tasks for different versions of the same function continiously
        #       destroying each other's Function Executors.
        #   - Each Function Executor rans at most 1 task concurrently.
        await state.wait_running_tasks_less(1)

        if state.function_id_with_version != _function_id_with_version(task):
            await state.destroy_function_executor()
            state.function_id_with_version = _function_id_with_version(task)
            # At this point the state belongs to the version of the function from the task
            # and there are no running tasks in the Function Executor.

    def _get_or_create_state(self, task: Task) -> FunctionExecutorState:
        id = _function_id_without_version(task)
        if id not in self._function_executor_states:
            state = FunctionExecutorState(
                function_id_with_version=_function_id_with_version(task),
                function_id_without_version=id,
            )
            self._function_executor_states[id] = state
        return self._function_executor_states[id]

    async def _run_task(
        self, state: FunctionExecutorState, task_input: TaskInput, logger: Any
    ) -> TaskOutput:
        runner: SingleTaskRunner = SingleTaskRunner(
            function_executor_state=state,
            task_input=task_input,
            function_executor_server_factory=self._factory,
            base_url=self._base_url,
            config_path=self._config_path,
            logger=logger,
        )
        return await runner.run()

    async def shutdown(self) -> None:
        # When shutting down there's no need to wait for completion of the running
        # FunctionExecutor tasks.
        while self._function_executor_states:
            id, state = self._function_executor_states.popitem()
            # At this point the state is not visible to new tasks.
            # Only ongoing tasks who read it already have a reference to it.
            await state.destroy_function_executor_not_locked()
            # The task running inside the Function Executor will fail because it's destroyed.
            # asyncio tasks waiting to run inside the Function Executor will get cancelled by
            # the caller's shutdown code.


def _function_id_with_version(task: Task) -> str:
    return f"versioned/{task.namespace}/{task.compute_graph}/{task.graph_version}/{task.compute_fn}"


def _function_id_without_version(task: Task) -> str:
    return f"not_versioned/{task.namespace}/{task.compute_graph}/{task.compute_fn}"
