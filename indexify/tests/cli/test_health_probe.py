import os
import subprocess
import unittest

import httpx
from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    test_graph_name,
    wait_executor_startup,
)


@tensorlake_function()
def crash_function(crash: bool) -> str:
    if crash:
        # os.kill(getpid(), signal.SIGKILL) won't work for container init process,
        # see https://stackoverflow.com/questions/21031537/sigkill-init-process-pid-1.
        # sys.exit(1) hangs the function for some unknown reason,
        # see some ideas at https://stackoverflow.com/questions/5422831/what-does-sys-exit-do-in-python.
        os._exit(1)
    return "success"


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

    def test_failure_after_function_executor_process_crash(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=crash_function,
        )
        graph = RemoteGraph.deploy(graph)

        # No need to verify if the default Executor health check is successful because other tests might make it fail already.

        # Create and crash the Function Executor twice.
        invocation_id = graph.run(block_until_done=True, crash=True)
        output = graph.output(invocation_id, "crash_function")
        self.assertEqual(len(output), 0)

        # Verify that the default Executor health check fails after the Function Executor crashed.
        response = httpx.get("http://localhost:7000/monitoring/health")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(),
            {
                "status": "nok",
                "message": "A Function Executor health check failed",
                "checker": "GenericHealthChecker",
            },
        )


if __name__ == "__main__":
    unittest.main()
