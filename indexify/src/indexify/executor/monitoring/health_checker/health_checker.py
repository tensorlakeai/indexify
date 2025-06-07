class HealthCheckResult:
    def __init__(self, checker_name: str, is_success: bool, status_message: str):
        self.checker_name = checker_name
        self.is_success = is_success
        self.status_message = status_message


class HealthChecker:
    """Abstract base class for health checkers."""

    async def check(self) -> HealthCheckResult:
        raise NotImplementedError("Subclasses must implement this method.")
