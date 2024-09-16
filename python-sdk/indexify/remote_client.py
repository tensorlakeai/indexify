import os
from typing import Any, List, Optional

import httpx
import yaml

from indexify.base_client import BaseClient
from indexify.error import Error
from indexify.exceptions import ApiException
from indexify.extraction_policy import ExtractionGraph
from indexify.functions_sdk.graph import ComputeGraphMetadata, Graph
from indexify.settings import DEFAULT_SERVICE_URL, DEFAULT_SERVICE_URL_HTTPS


class RemoteClient(BaseClient):
    def __init__(
        self,
        service_url: str = DEFAULT_SERVICE_URL,
        config_path: Optional[str] = None,
        namespace: str = "default",
        **kwargs,
    ):
        if os.environ.get("INDEXIFY_URL"):
            print("Using INDEXIFY_URL environment variable to connect to Indexify")
            service_url = os.environ["INDEXIFY_URL"]

        self.service_url = service_url
        self._client = httpx.Client()
        if config_path:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)

            if config.get("use_tls", False):
                tls_config = config["tls_config"]
                self._client = httpx.Client(
                    http2=True,
                    cert=(tls_config["cert_path"], tls_config["key_path"]),
                    verify=tls_config.get("ca_bundle_path", True),
                )

        self.namespace: str = namespace
        self.extraction_graphs: List[ExtractionGraph] = []
        self.labels: dict = {}
        self._service_url = service_url
        self._timeout = kwargs.get("timeout")

    def _request(self, method: str, **kwargs) -> httpx.Response:
        try:
            response = self._client.request(method, timeout=self._timeout, **kwargs)
            status_code = str(response.status_code)
            if status_code.startswith("4"):
                raise ApiException(
                    "status code: " + status_code + " request args: " + str(kwargs)
                )
            if status_code.startswith("5"):
                raise ApiException(response.text)
        except httpx.ConnectError:
            message = (
                f"Make sure the server is running and accesible at {self._service_url}"
            )
            error = Error(status="ConnectionError", message=message)
            print(error)
            raise error
        return response

    @classmethod
    def with_mtls(
        cls,
        cert_path: str,
        key_path: str,
        ca_bundle_path: Optional[str] = None,
        service_url: str = DEFAULT_SERVICE_URL_HTTPS,
        *args,
        **kwargs,
    ) -> "RemoteClient":
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
        client = RemoteClient(
            *args,
            **kwargs,
            service_url=service_url,
            http2=True,
            cert=client_certs,
            verify=verify_option,
        )
        return client

    def _get(self, endpoint: str, **kwargs) -> httpx.Response:
        return self._request("GET", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _post(self, endpoint: str, **kwargs) -> httpx.Response:
        return self._request("POST", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _put(self, endpoint: str, **kwargs) -> httpx.Response:
        return self._request("PUT", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _delete(self, endpoint: str, **kwargs) -> httpx.Response:
        return self._request("DELETE", url=f"{self._service_url}/{endpoint}", **kwargs)

    def _close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def register_graph(self, graph: Graph) -> ExtractionGraph:
        graph_metadata = graph.definition()
        serialized_code = graph.serialize()
        response = self._post(
            f"namespaces/{self.namespace}/compute_graphs",
            files={"code": serialized_code},
            data={"compute_graph": graph_metadata.model_dump_json(exclude_none=True)},
        )
        print(response.content.decode("utf-8"))
        response.raise_for_status()

    def graphs(self) -> List[str]:
        response = self._get(f"graphs")
        return response.json()["graphs"]

    def graph(self, name: str) -> ComputeGraphMetadata:
        response = self._get(f"namespaces/{self.namespace}/compute_graphs/{name}")
        return ComputeGraphMetadata(**response.json())

    def load_graph(self, name: str) -> Graph:
        response = self._get(
            f"internal/namespaces/{self.namespace}/compute_graphs/{name}/code"
        )
        return Graph.deserialize(response.content)

    def namespaces(self) -> List[str]:
        response = self._get(f"namespaces")
        namespaces_dict = response.json()["namespaces"]
        namespaces = []
        for item in namespaces_dict:
            namespaces.append(item["name"])
        return namespaces

    def create_namespace(self, namespace: str):
        self._post("namespaces", json={"namespace": namespace})

    def invoke_graph_with_object(self, graph: str, object: Any) -> str:
        pass
