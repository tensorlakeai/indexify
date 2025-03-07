from typing import Optional

from ...function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from ...function_executor.function_executor_status import FunctionExecutorStatus
from .health_checker import HealthChecker, HealthCheckResult

HEALTH_CHECKER_NAME = "GenericHealthChecker"


class GenericHealthChecker(HealthChecker):
    """A generic health checker that doesn't depend on machine type and other features of the environment.

    The health checker uses software signals available in all environments like Function Executor failure rates.
    """

    def __init__(self):
        self._function_executor_states: Optional[FunctionExecutorStatesContainer] = None
        self._function_executor_health_check_ever_failed = False

    def set_function_executor_states_container(
        self, states: FunctionExecutorStatesContainer
    ):
        self._function_executor_states = states

    async def check(self) -> HealthCheckResult:
        if self._function_executor_states is None:
            return HealthCheckResult(
                is_success=False,
                status_message="Function Executor states container was not provided yet",
                checker_name=HEALTH_CHECKER_NAME,
            )

        # Current health check policy and reasoning:
        # * A Function Executor health check failure is a strong signal that something is wrong
        #   either with:
        #   - The Function Code (a criticial software bug).
        #   - The Executor machine/container/VM (a software bug or malfunctioning local hardware).
        # * Critical Function Code bugs tend to get fixed eventually by users. What doesn't get fixed eventually
        #   is rare but recurring local Executor issues like hardware errors and software bugs in middleware like
        #   drivers.
        # * Such issues tend to get mitigated by automatically recreating the Executor machine/VM/container.
        # * So we fail whole Executor health check if a Function Executor health check ever failed to hint the users
        #   that we probably need to recreate the Executor machine/VM/container (unless there's a bug in Function
        #   code that user can investigate themself).
        await self._check_function_executors()
        if self._function_executor_health_check_ever_failed:
            return HealthCheckResult(
                is_success=False,
                status_message="A Function Executor health check failed",
                checker_name=HEALTH_CHECKER_NAME,
            )
        else:
            return HealthCheckResult(
                is_success=True,
                status_message="All Function Executors pass health checks",
                checker_name=HEALTH_CHECKER_NAME,
            )

    async def _check_function_executors(self):
        if self._function_executor_health_check_ever_failed:
            return

        async for state in self._function_executor_states:
            # No need to async lock the state to read a single value.
            if state.status in [
                FunctionExecutorStatus.UNHEALTHY,
                FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR,
                FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR,
            ]:
                self._function_executor_health_check_ever_failed = True
                return
