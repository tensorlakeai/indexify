from typing import Any, List, Optional

from indexify.functions_sdk.graph import Graph

from .http_client import IndexifyClient
from .settings import DEFAULT_SERVICE_URL


class RemoteGraph:
    def __init__(
        self,
        name: str,
        server_url: Optional[str] = DEFAULT_SERVICE_URL,
    ):
        self._name = name
        self._client = IndexifyClient(service_url=server_url)

    def run(self, block_until_done: bool = False, **kwargs) -> str:
        """
        Run the graph with the given inputs. The input is for the start function of the graph.
        :param block_until_done: If True, the function will block until the graph execution is complete.
        :param kwargs: The input to the start function of the graph. Pass the input as keyword arguments.
        :return: The invocation ID of the graph execution.

        Example:
        @indexify_function()
        def foo(x: int) -> int:
            return x + 1
        remote_graph = RemoteGraph.by_name("test")
        invocation_id = remote_graph.run(x=1)
        """
        return self._client.invoke_graph_with_object(
            self._name, block_until_done, **kwargs
        )

    def rerun(self):
        """
        Rerun the graph with the given invocation ID.
        :param invocation_id: The invocation ID of the graph execution.
        """
        self._client.rerun_graph(self._name)

    @classmethod
    def deploy(
        cls,
        g: Graph,
        additional_modules=[],
        server_url: Optional[str] = "http://localhost:8900",
    ):
        """
        Create a new RemoteGraph from a local Graph object.
        :param g: The local Graph object.
        :param server_url: The URL of the server where the graph will be registered.
        """
        client = IndexifyClient(service_url=server_url)
        client.register_compute_graph(g, additional_modules)
        return cls(name=g.name, server_url=server_url)

    @classmethod
    def by_name(cls, name: str, server_url: Optional[str] = "http://localhost:8900"):
        """
        Create a handle to call a RemoteGraph by name.
        :param name: The name of the graph.
        :param server_url: The URL of the server where the graph is registered.
        :return: A RemoteGraph object.
        """
        return cls(name=name, server_url=server_url)

    def output(
        self,
        invocation_id: str,
        fn_name: str,
    ) -> List[Any]:
        """
        Returns the extracted objects by a graph for an ingested object. If the extractor name is provided, only the objects extracted by that extractor are returned.
        If the extractor name is not provided, all the extracted objects are returned for the input object.
        invocation_id: str: The ID of the ingested object
        fn_name: Optional[str]: The name of the function whose output is to be returned if provided
        return: List[Any]: Output of the function.
        """

        return self._client.graph_outputs(
            graph=self._name,
            invocation_id=invocation_id,
            fn_name=fn_name,
        )
