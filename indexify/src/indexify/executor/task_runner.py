from typing import Any, Optional

from .api_objects import Task
from .function_executor.function_executor_state import (
    FunctionExecutorState,
    FunctionExecutorStatus,
)
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .function_executor.single_task_runner import SingleTaskRunner
from .function_executor.task_input import TaskInput
from .function_executor.task_output import TaskOutput
from .metrics.task_runner import (
    metric_task_policy_errors,
    metric_task_policy_latency,
    metric_task_policy_runs,
    metric_task_run_latency,
    metric_task_run_platform_errors,
    metric_task_runs,
    metric_tasks_blocked_by_policy,
    metric_tasks_blocked_by_policy_per_function_name,
    metric_tasks_running,
)


class TaskRunner:
    """Routes a task to its container following a scheduling policy.

    Due to the scheduling policy a task might be blocked for a while."""

    def __init__(
        self,
        executor_id: str,
        function_executor_server_factory: FunctionExecutorServerFactory,
        base_url: str,
        function_executor_states: FunctionExecutorStatesContainer,
        config_path: Optional[str],
    ):
        self._executor_id: str = executor_id
        self._factory: FunctionExecutorServerFactory = function_executor_server_factory
        self._base_url: str = base_url
        self._config_path: Optional[str] = config_path
        self._function_executor_states: FunctionExecutorStatesContainer = (
            function_executor_states
        )

    async def run(self, task_input: TaskInput, logger: Any) -> TaskOutput:
        logger = logger.bind(module=__name__)
        state: Optional[FunctionExecutorState] = None

        try:
            with (
                metric_task_policy_errors.count_exceptions(),
                metric_tasks_blocked_by_policy.track_inprogress(),
                metric_tasks_blocked_by_policy_per_function_name.labels(
                    function_name=task_input.task.compute_fn
                ).track_inprogress(),
                metric_task_policy_latency.time(),
            ):
                metric_task_policy_runs.inc()
                state = await self._acquire_function_executor_for_task_execution(
                    task_input, logger
                )

            with (
                metric_task_run_platform_errors.count_exceptions(),
                metric_tasks_running.track_inprogress(),
                metric_task_run_latency.time(),
            ):
                metric_task_runs.inc()
                return await self._run_task(state, task_input, logger)
        except Exception as e:
            logger.error(
                "failed running the task:",
                exc_info=e,
            )
            return TaskOutput.internal_error(
                task_id=task_input.task.id,
                namespace=task_input.task.namespace,
                graph_name=task_input.task.compute_graph,
                function_name=task_input.task.compute_fn,
                graph_version=task_input.task.graph_version,
                graph_invocation_id=task_input.task.invocation_id,
            )
        finally:
            if state is not None:
                state.lock.release()

    async def _acquire_function_executor_for_task_execution(
        self, task_input: TaskInput, logger: Any
    ) -> FunctionExecutorState:
        """Waits untils the task acquires a Function Executor state where the task can run.

        The returned Function Executor state is locked and the caller is responsible for releasing the lock.
        """
        logger.info("task is blocked by policy")
        state = await self._function_executor_states.get_or_create_state(
            id=_function_id_without_version(task_input.task),
            namespace=task_input.task.namespace,
            graph_name=task_input.task.compute_graph,
            graph_version=task_input.task.graph_version,
            function_name=task_input.task.compute_fn,
            image_uri=task_input.task.image_uri,
        )
        await state.lock.acquire()

        try:
            await self._run_task_policy(state, task_input.task)
            return state
        except Exception:
            state.lock.release()
            raise

    async def _run_task_policy(self, state: FunctionExecutorState, task: Task) -> None:
        """Runs the task policy until the task can run on the Function Executor.

        On successful return the Function Executor status is either IDLE or DESTROYED.
        """
        # Current policy for running tasks:
        #   - There can only be a single Function Executor per function regardless of function versions.
        #   --  If a Function Executor already exists for a different function version then wait until
        #       all the tasks finish in the existing Function Executor and then destroy it.
        #   --  This prevents failed tasks for different versions of the same function continiously
        #       destroying each other's Function Executors.
        #   - Each Function Executor rans at most 1 task concurrently.
        await state.wait_status(
            [
                FunctionExecutorStatus.DESTROYED,
                FunctionExecutorStatus.IDLE,
                FunctionExecutorStatus.UNHEALTHY,
                FunctionExecutorStatus.SHUTDOWN,
            ]
        )
        # We only shutdown the Function Executor on full Executor shutdown so it's fine to raise error here.
        if state.status == FunctionExecutorStatus.SHUTDOWN:
            raise Exception("Function Executor state is shutting down")

        if state.status == FunctionExecutorStatus.UNHEALTHY:
            await state.destroy_function_executor()

        if state.graph_version == task.graph_version:
            return  # All good, we can run on this Function Executor.

        if state.status in [FunctionExecutorStatus.IDLE]:
            await state.destroy_function_executor()

        state.graph_version = task.graph_version
        # At this point the state belongs to the version of the function from the task
        # and there are no running tasks in the Function Executor.

    async def _run_task(
        self, state: FunctionExecutorState, task_input: TaskInput, logger: Any
    ) -> TaskOutput:
        logger.info("task execution started")
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


def _function_id_without_version(task: Task) -> str:
    return f"not_versioned/{task.namespace}/{task.compute_graph}/{task.compute_fn}"
