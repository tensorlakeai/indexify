from typing import Optional, Union

import httpx
import yaml
from httpx import AsyncClient, Client


def get_httpx_client(
    config_path: Optional[str] = None, make_async: Optional[bool] = False
) -> AsyncClient | Client:
    """
    Creates and returns an httpx.Client instance, optionally configured with TLS settings from a YAML config file.

    The function creates a basic httpx.Client by default. If a config path is provided and the config specifies
    'use_tls' as True, it creates a TLS-enabled client with HTTP/2 support using the provided certificate settings.
    To get https.AsyncClient, provide make_async as True.

    Args:
        config_path (Optional[str]): Path to a YAML configuration file. If provided, the file should contain TLS
            configuration with the following structure:
            {
                "use_tls": bool,
                "tls_config": {
                    "cert_path": str,  # Path to client certificate
                    "key_path": str,   # Path to client private key
                    "ca_bundle_path": str  # Optional: Path to CA bundle for verification
                }
            }
        make_async (Optional[bool]): Whether to make an asynchronous httpx client instance.

    Returns:
        httpx.Client: An initialized httpx client instance, either with basic settings or TLS configuration
                     if specified in the config file.

    Example:
        # Basic client without TLS
        client = get_httpx_client()

        # Client with TLS configuration from config file
        client = get_httpx_client("/path/to/config.yaml")
    """
    if config_path:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        if config.get("use_tls", False):
            print(f"Configuring client with TLS config: {config}")
            tls_config = config["tls_config"]
            return get_sync_or_async_client(make_async, **tls_config)
    return get_sync_or_async_client(make_async)


def get_sync_or_async_client(
    make_async: Optional[bool] = False,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    ca_bundle_path: Optional[str] = None,
) -> AsyncClient | Client:
    """
    Creates and returns either a synchronous or asynchronous httpx client with optional TLS configuration.

    Args:
        make_async (Optional[bool]): If True, returns an AsyncClient; if False, returns a synchronous Client.
            Defaults to False.
        cert_path (Optional[str]): Path to the client certificate file. Required for TLS configuration
            when key_path is also provided.
        key_path (Optional[str]): Path to the client private key file. Required for TLS configuration
            when cert_path is also provided.
        ca_bundle_path (Optional[str]): Path to the CA bundle file for certificate verification.
            If not provided, defaults to system CA certificates.
    """
    if make_async:
        if cert_path and key_path:
            return httpx.AsyncClient(
                http2=True,
                cert=(cert_path, key_path),
                verify=ca_bundle_path if ca_bundle_path else True,
            )
        else:
            return httpx.AsyncClient()
    else:
        if cert_path and key_path:
            return httpx.Client(
                http2=True,
                cert=(cert_path, key_path),
                verify=ca_bundle_path if ca_bundle_path else True,
            )
        else:
            return httpx.Client()
