import httpx
from .repository import Repository
from .settings import DEFAULT_SERVICE_URL
from .extractor import Extractor

from typing import List, Optional


class IndexifyClient:
    """
    IndexifyClient is the main entry point for the SDK.
    For the full list of client features, see the
    [httpx Client documentation](https://www.python-httpx.org/api/#client).

    :param service_url: The URL of the Indexify service to connect to.
    :param args: Arguments to pass to the httpx.Client constructor
    :param kwargs: Keyword arguments to pass to the httpx.Client constructor

    Example usage:
    ```
    from indexify import IndexifyClient

    client = IndexifyClient()
    assert client.heartbeat() == True
    ```
    """
    def __init__(self,
                 service_url: str = DEFAULT_SERVICE_URL,
                 *args,
                 **kwargs
                 ):
        self._service_url = service_url
        self._client = httpx.Client(*args, **kwargs)

    @classmethod
    def with_mtls(cls, cert_path: str, key_path: str, ca_bundle_path: Optional[str] = None,
                 service_url: str = DEFAULT_SERVICE_URL, *args, **kwargs) -> "IndexifyClient":
        """
        Create a client with mutual TLS authentication. Also enables HTTP/2,
        which is required for mTLS.
        NOTE: mTLS must be enabled on the Indexify service for this to work.

        :param cert_path: Path to the client certificate. Resolution handled by httpx.
        :param key_path: Path to the client key. Resolution handled by httpx.
        :param args: Arguments to pass to the httpx.Client constructor
        :param kwargs: Keyword arguments to pass to the httpx.Client constructor
        :return: A client with mTLS authentication

        Example usage:
        ```
        from indexify import IndexifyClient

        client = IndexifyClient.with_mtls(
            cert_path="/path/to/cert.pem",
            key_path="/path/to/key.pem",
        )
        assert client.heartbeat() == True
        ```
        """
        if not (cert_path and key_path):
            raise ValueError("Both cert and key must be provided for mTLS")

        client_certs = (cert_path, key_path)
        verify_option = ca_bundle_path if ca_bundle_path else True
        client = IndexifyClient(*args, **kwargs, service_url=service_url, http2=True, cert=client_certs, verify=verify_option)
        return client

    def _request(self, method: str, **kwargs) -> httpx.Response:
        response = self._client.request(method, **kwargs)
        response.raise_for_status()
        return response

    def get(self, endpoint: str, **kwargs) -> httpx.Response:
        """
        Make a GET request to the Indexify service.

        :param endpoint: The endpoint to make the request to.

        Example usage:
        ```
        from indexify import IndexifyClient

        client = IndexifyClient()
        response = client.get("repositories")
        print(response.json())
        ```
        """
        return self._request("GET", url=f"{self._service_url}/{endpoint}", **kwargs)

    def post(self, endpoint: str, **kwargs) -> httpx.Response:
        """
        Make a POST request to the Indexify service.

        :param endpoint: The endpoint to make the request to.

        Example usage:

        ```
        from indexify import IndexifyClient

        client = IndexifyClient()
        response = client.post("repositories", json={"name": "my-repo"})
        print(response.json())
        ```
        """
        return self._request("POST", url=f"{self._service_url}/{endpoint}", **kwargs)

    def put(self, endpoint: str, **kwargs) -> httpx.Response:
        # Not Implemented
        raise NotImplementedError

    def delete(self, endpoint: str, **kwargs) -> httpx.Response:
        # Not Implemented
        raise NotImplementedError

    def close(self):
        """
        Close the underlying httpx.Client.
        """
        self._client.close()

    # __enter__ and __exit__ allow the client to be used as a context manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def heartbeat(self, heartbeat_response="Indexify Server") -> bool:
        """
        Check if the Indexify service is alive.
        """
        response = self.get(f"")
        # Server responds with text: "Indexify Server"
        return response.text == heartbeat_response

    def repositories(self) -> list[Repository]:
        """
        Get a list of all repositories.
        """
        response = self.get(f"repositories")
        repositories_dict = response.json()["repositories"]
        repositories = []
        for rd in repositories_dict:
            repositories.append(Repository(rd["name"], self._service_url))
        return repositories

    def create_repository(
        self, name: str, extractor_bindings: list = [], metadata: dict = {}
    ) -> Repository:
        """
        Create a new repository.
        """
        req = {
            "name": name,
            "extractor_bindings": extractor_bindings,
            "metadata": metadata,
        }
        response = self.post(f"repositories", json=req)
        return Repository(name, self._service_url)

    def get_repository(self, name: str) -> Repository:
        """
        Get a repository by name.
        """
        return Repository(name, self._service_url)

    def extractors(self) -> List[Extractor]:
        """
        Get a list of all extractors.
        """
        response = self.get(f"extractors")
        extractors_dict = response.json()["extractors"]
        extractors = []
        for ed in extractors_dict:
            extractors.append(Extractor.from_dict(ed))
        return extractors

