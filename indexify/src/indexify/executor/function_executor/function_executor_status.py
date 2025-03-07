from enum import Enum


class FunctionExecutorStatus(Enum):
    """Status of a Function Executor.

    Each status lists transitions allowed to it.
    """

    # DESTROYED -> STARTING_UP
    STARTING_UP = "Starting Up"
    # STARTING_UP -> STARTUP_FAILED_CUSTOMER_ERROR
    STARTUP_FAILED_CUSTOMER_ERROR = "Startup Failed (Customer Error)"
    # STARTING_UP -> STARTUP_FAILED_PLATFORM_ERROR
    STARTUP_FAILED_PLATFORM_ERROR = "Startup Failed (Platform Error)"
    # STARTING_UP -> IDLE
    # RUNNING_TASK -> IDLE
    IDLE = "Idle"
    # IDLE -> RUNNING_TASK
    RUNNING_TASK = "Running Task"
    # IDLE -> UNHEALTHY
    # RUNNING_TASK -> UNHEALTHY
    UNHEALTHY = "Unhealthy"
    # STARTUP_FAILED_CUSTOMER_ERROR -> DESTROYING
    # STARTUP_FAILED_PLATFORM_ERROR -> DESTROYING
    # UNHEALTHY -> DESTROYING
    # IDLE -> DESTROYING
    DESTROYING = "Destroying"
    # DESTROYED (initial status)
    # DESTROYING -> DESTROYED
    DESTROYED = "Destroyed"
    # Any state -> SHUTDOWN
    SHUTDOWN = "Shutdown"  # Permanent stop state


def is_status_change_allowed(
    current_status: FunctionExecutorStatus, new_status: FunctionExecutorStatus
) -> bool:
    """Returns True if the transition is allowed."""
    allowed_transitions = {
        FunctionExecutorStatus.DESTROYED: [
            FunctionExecutorStatus.DESTROYED,
            FunctionExecutorStatus.STARTING_UP,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.STARTING_UP: [
            FunctionExecutorStatus.STARTING_UP,
            FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR,
            FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR,
            FunctionExecutorStatus.IDLE,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR: [
            FunctionExecutorStatus.STARTUP_FAILED_CUSTOMER_ERROR,
            FunctionExecutorStatus.DESTROYING,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR: [
            FunctionExecutorStatus.STARTUP_FAILED_PLATFORM_ERROR,
            FunctionExecutorStatus.DESTROYING,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.IDLE: [
            FunctionExecutorStatus.IDLE,
            FunctionExecutorStatus.RUNNING_TASK,
            FunctionExecutorStatus.UNHEALTHY,
            FunctionExecutorStatus.DESTROYING,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.RUNNING_TASK: [
            FunctionExecutorStatus.RUNNING_TASK,
            FunctionExecutorStatus.IDLE,
            FunctionExecutorStatus.UNHEALTHY,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.UNHEALTHY: [
            FunctionExecutorStatus.UNHEALTHY,
            FunctionExecutorStatus.DESTROYING,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.DESTROYING: [
            FunctionExecutorStatus.DESTROYING,
            FunctionExecutorStatus.DESTROYED,
            FunctionExecutorStatus.SHUTDOWN,
        ],
        FunctionExecutorStatus.SHUTDOWN: [
            FunctionExecutorStatus.SHUTDOWN
        ],  # No transitions allowed from SHUTDOWN
    }

    return new_status in allowed_transitions.get(current_status, [])
