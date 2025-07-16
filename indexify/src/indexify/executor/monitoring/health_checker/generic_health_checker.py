from typing import Optional

from .health_checker import HealthChecker, HealthCheckResult

_HEALTH_CHECKER_NAME = "GenericHealthChecker"


class GenericHealthChecker(HealthChecker):
    """A generic health checker that doesn't depend on machine type and other features of the environment.

    The health checker uses software signals available in all environments like Function Executor failure rates.
    """

    def __init__(self):
        self._server_connection_unhealthy_status_message: Optional[str] = None

    def server_connection_state_changed(self, is_healthy: bool, status_message: str):
        """Handle changes in server connection state."""
        if is_healthy:
            self._server_connection_unhealthy_status_message = None
        else:
            self._server_connection_unhealthy_status_message = status_message

    async def check(self) -> HealthCheckResult:
        if self._server_connection_unhealthy_status_message is not None:
            return HealthCheckResult(
                is_success=False,
                status_message=self._server_connection_unhealthy_status_message,
                checker_name=_HEALTH_CHECKER_NAME,
            )

        return HealthCheckResult(
            is_success=True,
            status_message="Successful",
            checker_name=_HEALTH_CHECKER_NAME,
        )
