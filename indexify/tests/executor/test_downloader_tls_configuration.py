import unittest
from unittest.mock import mock_open, patch

from constants import (
    ca_bundle_path,
    cert_path,
    code_path,
    config_path,
    key_path,
    server_address,
)

from indexify.executor.downloader import Downloader


class TestDownloaderTLSConfiguration(unittest.TestCase):
    @patch("httpx.AsyncClient")
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
    def test_creates_httpx_client_with_mTLS(self, mock_file, mock_client):
        downloader = Downloader(
            code_path=code_path,
            base_url=f"https://{server_address}",
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

    @patch("httpx.AsyncClient")
    @patch("builtins.open", new_callable=mock_open, read_data="""use_tls: false""")
    def test_creates_httpx_client_without_mTLS(self, mock_file, mock_client):
        downloader = Downloader(
            code_path=code_path,
            base_url=f"http://{server_address}",
            config_path=config_path,
        )
        mock_file.assert_called()
        mock_client.assert_called_with()


if __name__ == "__main__":
    unittest.main()
