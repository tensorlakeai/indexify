from typing import Optional

import httpx
import yaml

def get_httpx_client(config_path: Optional[str] = None) -> httpx.Client:
    """
    Creates and returns an httpx.Client instance, optionally configured with TLS settings from a YAML config file.

    The function creates a basic httpx.Client by default. If a config path is provided and the config specifies
    'use_tls' as True, it creates a TLS-enabled client with HTTP/2 support using the provided certificate settings.

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

    Returns:
        httpx.Client: An initialized httpx client instance, either with basic settings or TLS configuration
                     if specified in the config file.

    Example:
        # Basic client without TLS
        client = get_httpx_client()

        # Client with TLS configuration from config file
        client = get_httpx_client("/path/to/config.yaml")
    """
    client = httpx.Client()
    if config_path:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        if config.get("use_tls", False):
            print(f'Configuring client with TLS config: {config}')
            tls_config = config["tls_config"]
            client = httpx.Client(
                http2=True,
                cert=(tls_config["cert_path"], tls_config["key_path"]),
                verify=tls_config.get("ca_bundle_path", True),
            )
    return client
