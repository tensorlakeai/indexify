from typing import Any, Optional

from .api_objects import Task
from .function_executor.function_executor_state import FunctionExecutorState
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
    function_id_with_version,
    function_id_without_version,
)
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
        executor_id: str,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        disable_automatic_function_executor_management: bool,
        function_executor_states: FunctionExecutorStatesContainer,
        config_path: Optional[str],
    ):
        self._executor_id: str = executor_id
        self._factory: FunctionExecutorServerFactory = function_executor_server_factory
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._disable_automatic_function_executor_management: bool = (
            disable_automatic_function_executor_management
        )
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )

    async def run(self, task_input: TaskInput, logger: Any) -> TaskOutput:
        logger = logger.bind(module=__name__)
        try:
            return await self._run(task_input, logger)
        except Exception as e:
            logger.error(
                "failed running the task:",
                exc_info=e,
            )
            return TaskOutput.internal_error(task_input.task)

    async def _run(self, task_input: TaskInput, logger: Any) -> TaskOutput:
        state = await self._function_executor_states.get_or_create_state(
            task_input.task
        )
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

        if self._disable_automatic_function_executor_management:
            return  # Disable Function Executor destroy in manual management mode.

        if state.function_id_with_version != function_id_with_version(task):
            await state.destroy_function_executor()
            state.function_id_with_version = function_id_with_version(task)
            # At this point the state belongs to the version of the function from the task
            # and there are no running tasks in the Function Executor.

    async def _run_task(
        self, state: FunctionExecutorState, task_input: TaskInput, logger: Any
    ) -> TaskOutput:
        runner: SingleTaskRunner = SingleTaskRunner(
            executor_id=self._executor_id,
            function_executor_state=state,
            task_input=task_input,
            function_executor_server_factory=self._factory,
            base_url=self._base_url,
            config_path=self._config_path,
            logger=logger,
        )
        return await runner.run()

    async def shutdown(self) -> None:
        pass
