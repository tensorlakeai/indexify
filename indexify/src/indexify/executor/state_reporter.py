from typing import Any, Optional

from .function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)


class ExecutorStateReporter:
    def __init__(self, function_executor_states: FunctionExecutorStatesContainer):
        self._function_executor_states = function_executor_states

    async def run(self, logger: Any):
        logger = logger.bind(module=__name__)

    async def shutdown(self):
        pass
