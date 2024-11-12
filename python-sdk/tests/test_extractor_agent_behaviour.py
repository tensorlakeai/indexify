import ssl
import unittest
from pathlib import Path
from unittest.mock import mock_open, patch

from test_constants import (
    ca_bundle_path,
    cert_path,
    config_path,
    key_path,
    service_url,
    tls_config,
)

from indexify.executor.agent import ExtractorAgent


class TestExtractorAgent(unittest.TestCase):
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
    def test_tls_configuration(self, mock_client, mock_file):
        # Create an instance of ExtractorAgent with the mock config
        agent = ExtractorAgent(
            executor_id="unit-test",
            num_workers=1,
            code_path=Path("test"),
            server_addr=service_url,
            config_path=config_path,
        )

        # Verify that the correct file was loaded from the config_path
        mock_file.assert_called()

        # Verify TLS config in httpsx Client
        mock_client.assert_called_with(
            http2=True,
            cert=(cert_path, key_path),
            verify=ca_bundle_path,
        )

        # Verify TLS config in Agent
        self.assertEqual(agent._server_addr, service_url)
        self.assertEqual(agent._protocol, "https")

    def test_no_tls_configuration(self):
        # Create an instance of ExtractorAgent without TLS
        agent = ExtractorAgent(
            executor_id="unit-test",
            num_workers=1,
            code_path=Path("test"),
            server_addr="localhost:8900",
        )

        # Verify the protocol is set to "http"
        self.assertEqual(agent._protocol, "http")


if __name__ == "__main__":
    unittest.main()
