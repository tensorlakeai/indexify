from ...function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)


class HealthCheckResult:
    def __init__(self, checker_name: str, is_success: bool, status_message: str):
        self.checker_name = checker_name
        self.is_success = is_success
        self.status_message = status_message


class HealthChecker:
    """Abstract base class for health checkers."""

    def set_function_executor_states_container(
        self, states: FunctionExecutorStatesContainer
    ):
        """Provides function executor states to this health checker so it can use them in the health checks."""
        raise NotImplementedError("Subclasses must implement this method.")

    async def check(self) -> HealthCheckResult:
        raise NotImplementedError("Subclasses must implement this method.")
