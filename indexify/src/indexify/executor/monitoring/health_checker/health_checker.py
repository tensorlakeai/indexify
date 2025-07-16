class HealthCheckResult:
    def __init__(self, checker_name: str, is_success: bool, status_message: str):
        self.checker_name: str = checker_name
        self.is_success: bool = is_success
        self.status_message: str = status_message


class HealthChecker:
    """Abstract base class for health checkers."""

    def server_connection_state_changed(self, is_healthy: bool, status_message: str):
        """Handle changes in server connection state."""
        raise NotImplementedError("Subclasses must implement this method.")

    async def check(self) -> HealthCheckResult:
        raise NotImplementedError("Subclasses must implement this method.")
