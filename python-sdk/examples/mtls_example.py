import os

from indexify import IndexifyClient


def run_example():
    """
    Instructions:

    Before running this example, go through the following steps:

    First, go to the [Developing Indexify](https://getindexify.ai/develop/) page and follow the instructions
    to set up a local Indexify service.

    When that is complete, run `make local-dev-tls-insecure` in the root of the repo. That
    will create a self-signed certificate and keys for both the client and server, at ./.dev-tls/*.

    Ensure the TLS settings are set up correctly in local_server_config.yaml, and set the following
    environment variables:

    INDEXIFY_CERT_PATH=/path/to/repo/.dev-tls/client.crt
    INDEXIFY_KEY_PATH=/path/to/repo/.dev-tls/client.key
    INDEXIFY_CA_PATH=/path/to/repo/.dev-tls/ca.crt

    If the custom CA is not set, the client will use the system CA bundle, which may not be what you want.
    """
    # Get cert path and key path from environment variables
    cert_path = os.environ.get("INDEXIFY_CERT_PATH")
    key_path = os.environ.get("INDEXIFY_KEY_PATH")
    ca_path = os.environ.get("INDEXIFY_CA_PATH", None)

    # Get the service url from env or default to https://localhost:8900
    service_url = os.environ.get("INDEXIFY_SERVICE_URL", "https://localhost:8900")

    print(f"Using service url: {service_url}")
    print(f"Using cert path: {cert_path}")
    print(f"Using key path: {key_path}")
    print(f"Using ca path: {ca_path}")

    # If they aren't set, raise an error
    if not (cert_path and key_path):
        raise ValueError(
            "Both cert and key must be provided as environment variables for mTLS: INDEXIFY_CERT_PATH, INDEXIFY_KEY_PATH"
        )

    client = IndexifyClient.with_mtls(
        service_url=service_url,
        cert_path=cert_path,
        key_path=key_path,
        ca_bundle_path=ca_path,
    )
    if client.heartbeat():
        print("mTLS connection is good!")
    else:
        raise Exception("mTLS connection is not good!")


if __name__ == "__main__":
    run_example()
