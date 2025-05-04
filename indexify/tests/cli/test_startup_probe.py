import subprocess
import unittest

import httpx
from testing import ExecutorProcessContextManager, wait_executor_startup
class TestStartupProbe(unittest.TestCase):
    def test_success(self):
        with ExecutorProcessContextManager(
            [
                "--monitoring-server-port",
                "7001",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)
            response = httpx.get(f"http://localhost:7001/monitoring/startup")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"status": "ok"})

    def test_failure(self):
        # There's currently no way to reliably slow down Executor startup so this test is empty for now.
        pass

if __name__ == "__main__":
    unittest.main()
