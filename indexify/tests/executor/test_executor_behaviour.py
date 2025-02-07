import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

from constants import ca_bundle_path, cert_path, config_path, key_path, server_address

from indexify.executor.executor import Executor
from indexify.executor.monitoring.health_checker.health_checker import (
    HealthChecker,
    HealthCheckResult,
)


class StubHealthChecker(HealthChecker):
    def set_function_executor_states_container(self, _):
        pass

    async def check(self) -> HealthCheckResult:
        return HealthCheckResult(
            is_success=True,
            status_message="Stub health checker",
            checker_name="StubHealthChecker",
        )


class TestExecutor(unittest.TestCase):
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="""
                use_tls: true
                tls_config:
                    ca_bundle_path: /path/to/ca_bundle.pem
                    cert_path: /path/to/cert.pem
                    key_path: /path/to/key.pem
                """,
    )
    @patch("httpx.Client")
    @patch("httpx.AsyncClient")
    def test_tls_configuration(self, mock_async_client, mock_sync_client, mock_file):
        # Create an instance of Executor with the mock config
        executor = Executor(
            id="unit-test",
            version="0.1.0",
            code_path=Path("test"),
            health_checker=StubHealthChecker(),
            function_allowlist=None,
            function_executor_server_factory=None,
            server_addr=server_address,
            config_path=config_path,
            monitoring_server_host="localhost",
            monitoring_server_port=7000,
            disable_automatic_function_executor_management=False,
        )

        # Verify that the correct file was loaded from the config_path
        mock_file.assert_called()

        # Verify TLS config in httpsx Client
        mock_sync_client.assert_called_with(
            http2=True,
            cert=(cert_path, key_path),
            verify=ca_bundle_path,
        )

        # Verify TLS config in httpsx AsyncClient
        mock_async_client.assert_called_with(
            http2=True,
            cert=(cert_path, key_path),
            verify=ca_bundle_path,
        )

        # Verify TLS config in Executor
        self.assertEqual(executor._server_addr, server_address)
        self.assertTrue(executor._base_url.startswith("https://"))

    def test_no_tls_configuration(self):
        # Create an instance of Executor without TLS
        executor = Executor(
            id="unit-test",
            version="0.1.0",
            code_path=Path("test"),
            health_checker=StubHealthChecker(),
            function_allowlist=None,
            function_executor_server_factory=None,
            server_addr=server_address,
            config_path=None,
            monitoring_server_host="localhost",
            monitoring_server_port=7000,
            disable_automatic_function_executor_management=False,
        )

        # Verify the protocol is set to "http"
        self.assertTrue(executor._base_url.startswith("http://"))


if __name__ == "__main__":
    unittest.main()
