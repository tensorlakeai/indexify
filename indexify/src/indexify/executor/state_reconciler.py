from typing import Any, Optional

from .function_executor.function_executor_state import FunctionExecutorState
from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
    function_id_with_version,
)
from .function_executor.server.function_executor_server_factory import (
    FunctionExecutorServerFactory,
)
from .function_executor.single_task_runner import SingleTaskRunner
from .function_executor.task_input import TaskInput
from .function_executor.task_output import TaskOutput


class ExecutorStateReconciler:
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

    async def run(self, logger: Any):
        logger = logger.bind(module=__name__)

    async def shutdown(self):
        pass
