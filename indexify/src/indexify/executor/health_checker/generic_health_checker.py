from typing import Optional

from ..function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from .health_checker import HealthChecker, HealthCheckResult


class GenericHealthChecker(HealthChecker):
    """A generic health checker that doesn't depend on machine type and other features of the environment.

    The health checker uses software signals available in all environments like Function Executor failure rates.
    """

    def __init__(self):
        self._function_executor_states: Optional[FunctionExecutorStatesContainer] = None

    def set_function_executor_states_container(
        self, states: FunctionExecutorStatesContainer
    ):
        self._function_executor_states = states

    async def check(self) -> HealthCheckResult:
        if self._function_executor_states is None:
            return HealthCheckResult(
                is_success=False,
                status_message="Function executor states container was not provided yet",
                checker_name="GenericHealthChecker",
            )

        return HealthCheckResult(
            is_success=False,
            status_message="Not implemented",
            checker_name="GenericHealthChecker",
        )
