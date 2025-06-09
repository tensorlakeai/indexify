from .health_checker import HealthChecker, HealthCheckResult

HEALTH_CHECKER_NAME = "GenericHealthChecker"


class GenericHealthChecker(HealthChecker):
    """A generic health checker that doesn't depend on machine type and other features of the environment.

    The health checker uses software signals available in all environments like Function Executor failure rates.
    """

    def __init__(self):
        pass

    async def check(self) -> HealthCheckResult:
        return HealthCheckResult(
            is_success=True,
            status_message="The health check is always successful",
            checker_name=HEALTH_CHECKER_NAME,
        )
