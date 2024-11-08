import ssl
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open

from indexify.executor.agent import ExtractorAgent

class TestExtractorAgent(unittest.TestCase):

    @patch(
        'builtins.open',
        new_callable=mock_open,
        read_data='''
                use_tls: true
                tls_config:
                    ca_bundle_path: /path/to/ca_bundle.pem
                    cert_path: /path/to/cert.pem
                    key_path: /path/to/key.pem
                '''
    )
    @patch('ssl.create_default_context')
    @patch('httpx.Client')
    def test_tls_configuration(
        self,
        mock_client,
        mock_create_default_context,
        mock_file
    ):
        tls_config = {
            "use_tls": True,
            "tls_config": {
                "ca_bundle_path": "/path/to/ca_bundle.pem",
                "cert_path": "/path/to/cert.pem",
                "key_path": "/path/to/key.pem"
        }}

        # Create an instance of ExtractorAgent with the mock config
        service_url = "localhost:8900"
        config_path = "test"
        agent = ExtractorAgent(
            executor_id="unit-test",
            num_workers=1,
            code_path=Path("test"),
            server_addr=service_url,
            config_path=config_path
        )

        # Verify that the correct file was loaded from the config_path
        mock_file.assert_called()

        # Verify that the SSL context was created correctly
        mock_create_default_context.assert_called_with(ssl.Purpose.SERVER_AUTH,
            cafile='/path/to/ca_bundle.pem')
        agent._ssl_context.load_cert_chain.assert_called_with(
            certfile='/path/to/cert.pem', keyfile='/path/to/key.pem')

        # Verify TLS config in IndexifyClient
        mock_client.assert_called_with(
            http2=True,
            cert=(tls_config["tls_config"]["cert_path"], tls_config["tls_config"]["key_path"]),
            verify=tls_config["tls_config"]["ca_bundle_path"],
        )

        # Verify TLS config in Agent
        self.assertTrue(agent._use_tls)
        self.assertEqual(agent._config, tls_config)
        self.assertEqual(agent._server_addr, "localhost:8900")
        self.assertEqual(agent._protocol, "wss")
        self.assertEqual(agent._tls_config, tls_config["tls_config"])

    def test_no_tls_configuration(self):
        # Create an instance of ExtractorAgent without TLS
        agent = ExtractorAgent(
            executor_id="unit-test",
            num_workers=1,
            code_path=Path("test"),
            server_addr="localhost:8900",
        )

        # Verify that TLS is disabled
        self.assertFalse(agent._use_tls)

        # Verify that the SSL context is None
        self.assertIsNone(agent._ssl_context)

        # Verify the protocol is set to "http"
        self.assertEqual(agent._protocol, 'http')

if __name__ == '__main__':
    unittest.main()
