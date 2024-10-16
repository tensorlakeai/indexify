import json
import os
from typing import Any, Dict, List, Optional

import cloudpickle
import httpx
import msgpack
import yaml
from httpx_sse import connect_sse
from pydantic import BaseModel, Json
from rich import print

from indexify.error import ApiException
from indexify.functions_sdk.data_objects import IndexifyData
from indexify.functions_sdk.graph import ComputeGraphMetadata, Graph
from indexify.functions_sdk.indexify_functions import IndexifyFunctionWrapper
from indexify.settings import DEFAULT_SERVICE_URL, DEFAULT_SERVICE_URL_HTTPS


class InvocationEventPayload(BaseModel):
    invocation_id: str
    fn_name: str
    task_id: str
    executor_id: Optional[str] = None
    outcome: Optional[str] = None


class InvocationEvent(BaseModel):
    event_name: str
    payload: InvocationEventPayload


class GraphOutputMetadata(BaseModel):
    id: str
    compute_fn: str


class GraphOutputs(BaseModel):
    outputs: List[GraphOutputMetadata]


class IndexifyClient:
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
        self.compute_graphs: List[Graph] = []
        self.labels: dict = {}
        self._service_url = service_url
        self._timeout = kwargs.get("timeout")
        self._graphs: Dict[str, Graph] = {}
        self._fns = {}

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
            ex = ApiException(message=message)
            print(ex)
            raise ex
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
    ) -> "IndexifyClient":
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
        client = IndexifyClient(
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

    def register_compute_graph(self, graph: Graph, additional_modules):
        graph_metadata = graph.definition()
        serialized_code = cloudpickle.dumps(graph.serialize(additional_modules))
        response = self._post(
            f"namespaces/{self.namespace}/compute_graphs",
            files={"code": serialized_code},
            data={"compute_graph": graph_metadata.model_dump_json(exclude_none=True)},
        )
        response.raise_for_status()
        self._graphs[graph.name] = graph
        for fn_name, fn in graph.nodes.items():
            self._fns[f"{graph.name}/{fn_name}"] = fn

    def graphs(self) -> List[str]:
        response = self._get(f"graphs")
        return response.json()["graphs"]

    def graph(self, name: str) -> ComputeGraphMetadata:
        response = self._get(f"namespaces/{self.namespace}/compute_graphs/{name}")
        return ComputeGraphMetadata(**response.json())

    def load_fn_wrapper(self, name: str, fn_name: str) -> IndexifyFunctionWrapper:
        response = self._get(
            f"internal/namespaces/{self.namespace}/compute_graphs/{name}/code"
        )
        pickled_functions_by_name = cloudpickle.loads(response.content)
        return IndexifyFunctionWrapper(
            cloudpickle.loads(pickled_functions_by_name[fn_name])
        )

    def namespaces(self) -> List[str]:
        response = self._get(f"namespaces")
        namespaces_dict = response.json()["namespaces"]
        namespaces = []
        for item in namespaces_dict:
            namespaces.append(item["name"])
        return namespaces

    @classmethod
    def new_namespace(
        cls, namespace: str, server_addr: Optional[str] = "http://localhost:8900"
    ):
        # Create a new client instance with the specified server address
        client = cls(service_url=server_addr)

        try:
            # Create the new namespace using the client
            client.create_namespace(namespace)
        except ApiException as e:
            print(f"Failed to create namespace '{namespace}': {e}")
            raise

        # Set the namespace for the newly created client
        client.namespace = namespace

        # Return the client instance with the new namespace
        return client

    def create_namespace(self, namespace: str):
        self._post("namespaces", json={"name": namespace})

    def logs(
        self, invocation_id: str, cg_name: str, fn_name: str, file: str
    ) -> Optional[str]:
        try:
            response = self._get(
                f"namespaces/{self.namespace}/compute_graphs/{cg_name}/invocations/{invocation_id}/fn/{fn_name}/logs/{file}"
            )
            response.raise_for_status()
            return response.content.decode("utf-8")
        except ApiException as e:
            print(f"failed to fetch logs: {e}")
            return None

    def rerun_graph(self, graph: str):
        self._post(f"namespaces/{self.namespace}/compute_graphs/{graph}/rerun")

    def invoke_graph_with_object(
        self, graph: str, block_until_done: bool = False, **kwargs
    ) -> str:
        ser_input = cloudpickle.dumps(kwargs)
        params = {"block_until_finish": block_until_done}
        with httpx.Client() as client:
            with connect_sse(
                client,
                "POST",
                f"{self.service_url}/namespaces/{self.namespace}/compute_graphs/{graph}/invoke_object",
                headers={"Content-Type": "application/cbor"},
                data=ser_input,
                params=params,
            ) as event_source:
                for sse in event_source.iter_sse():
                    obj = json.loads(sse.data)
                    for k, v in obj.items():
                        if k == "id":
                            return v
                        if k == "InvocationFinished":
                            return v["id"]
                        if k == "DiagnosticMessage":
                            message = v.get("message", None)
                            print(
                                f"[bold red]scheduler diagnostic: [/bold red]{message}"
                            )
                            continue
                        event_payload = InvocationEventPayload.model_validate(v)
                        event = InvocationEvent(event_name=k, payload=event_payload)
                        if (
                            event.event_name == "TaskCompleted"
                            and event.payload.outcome == "Failure"
                        ):
                            stdout = self.logs(
                                event.payload.invocation_id,
                                graph,
                                event.payload.fn_name,
                                "stdout",
                            )
                            stderr = self.logs(
                                event.payload.invocation_id,
                                graph,
                                event.payload.fn_name,
                                "stderr",
                            )
                            if stdout:
                                print(f"[bold red]stdout[/bold red]: \n {stdout}")
                            if stderr:
                                print(f"[bold red]stderr[/bold red]: \n {stderr}")
                        print(
                            f"[bold green]{event.event_name}[/bold green]: {event.payload}"
                        )
        raise Exception("invocation ID not returned")

    def _download_output(
        self,
        namespace: str,
        graph: str,
        invocation_id: str,
        fn_name: str,
        output_id: str,
    ) -> IndexifyData:
        response = self._get(
            f"namespaces/{namespace}/compute_graphs/{graph}/invocations/{invocation_id}/fn/{fn_name}/output/{output_id}",
        )
        response.raise_for_status()
        data_dict = msgpack.unpackb(response.content)
        return IndexifyData.model_validate(data_dict)

    def graph_outputs(
        self,
        graph: str,
        invocation_id: str,
        fn_name: Optional[str],
    ) -> List[Any]:
        """
        Returns the extracted objects by a graph for an ingested object. If the extractor name is provided, only the objects extracted by that extractor are returned.
        If the extractor name is not provided, all the extracted objects are returned for the input object.
        graph: str: The name of the graph
        invocation_id: str: The ID of the ingested object
        extractor_name: Optional[str]: The name of the extractor whose output is to be returned if provided
        block_until_done: bool = True: If True, the method will block until the extraction is done. If False, the method will return immediately.
        return: Union[Dict[str, List[Any]], List[Any]]: The extracted objects. If the extractor name is provided, the output is a list of extracted objects by the extractor. If the extractor name is not provided, the output is a dictionary with the extractor name as the key and the extracted objects as the value. If no objects are found, an empty list is returned.
        """
        fn_key = f"{graph}/{fn_name}"
        if fn_key not in self._fns:
            self._fns[fn_key] = self.load_fn_wrapper(graph, fn_name)
        response = self._get(
            f"namespaces/{self.namespace}/compute_graphs/{graph}/invocations/{invocation_id}/outputs",
        )
        response.raise_for_status()
        graph_outputs = GraphOutputs(**response.json())
        outputs = []
        for output in graph_outputs.outputs:
            if output.compute_fn == fn_name:
                indexify_data = self._download_output(
                    self.namespace, graph, invocation_id, fn_name, output.id
                )
                output = self._fns[fn_key].deserialize_fn_output(indexify_data)
                outputs.append(output)
        return outputs

    def invoke_graph_with_file(
        self, graph: str, path: str, metadata: Optional[Dict[str, Json]] = None
    ) -> str:
        """
        Invokes a graph with an input file. The file's mimetype is appropriately detected.
        graph: str: The name of the graph to invoke
        path: str: The path to the file to be ingested
        return: str: The ID of the ingested object
        """
        pass
