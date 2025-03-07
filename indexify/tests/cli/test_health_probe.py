import subprocess
import unittest

import httpx
import structlog
from testing import (
    ExecutorProcessContextManager,
    wait_executor_startup,
)

from indexify.executor.function_executor.function_executor_state import (
    FunctionExecutorState,
)
from indexify.executor.function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from indexify.executor.function_executor.function_executor_status import (
    FunctionExecutorStatus,
)
from indexify.executor.monitoring.health_checker.generic_health_checker import (
    GenericHealthChecker,
)
from indexify.executor.monitoring.health_checker.health_checker import HealthCheckResult


class TestHealthProbe(unittest.TestCase):
    def test_success(self):
        # Run a new Executor to not interfere with other tests that might turn the default Executor health checks into failing.
        with ExecutorProcessContextManager(
            [
                "--dev",
                "--ports",
                "60000",
                "60001",
                "--monitoring-server-port",
                "7001",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            response = httpx.get("http://localhost:7001/monitoring/health")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json(),
                {
                    "status": "ok",
                    "message": "All Function Executors pass health checks",
                    "checker": "GenericHealthChecker",
                },
            )


class TestGenericHealthChecker(unittest.IsolatedAsyncioTestCase):
    async def test_failure(self):
        health_checker = GenericHealthChecker()
        fe_states_container = FunctionExecutorStatesContainer(structlog.get_logger())
        health_checker.set_function_executor_states_container(fe_states_container)

        result: HealthCheckResult = await health_checker.check()
        self.assertTrue(result.is_success)
        self.assertEqual(result.checker_name, "GenericHealthChecker")
        self.assertEqual(
            result.status_message, "All Function Executors pass health checks"
        )

        state: FunctionExecutorState = await fe_states_container.get_or_create_state(
            "1", "ns", "graph", "1", "func", "image"
        )
        state.status = FunctionExecutorStatus.UNHEALTHY
        result: HealthCheckResult = await health_checker.check()
        self.assertFalse(result.is_success)
        self.assertEqual(result.checker_name, "GenericHealthChecker")
        self.assertEqual(
            result.status_message, "A Function Executor health check failed"
        )


if __name__ == "__main__":
    unittest.main()
