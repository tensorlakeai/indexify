from typing import Any, List, Optional

from indexify.functions_sdk.graph import ComputeGraphMetadata, Graph
from indexify.functions_sdk.graph_definition import ComputeGraphMetadata

from .http_client import IndexifyClient
from .settings import DEFAULT_SERVICE_URL


class RemoteGraph:
    def __init__(
        self,
        name: str,
        server_url: Optional[str] = DEFAULT_SERVICE_URL,
        client: Optional[IndexifyClient] = None,
    ):
        """
        Create a handle to call a RemoteGraph by name.

        Note: Use the class methods RemoteGraph.deploy or RemoteGraph.by_name to create a RemoteGraph object.

        :param name: The name of the graph.
        :param server_url: The URL of the server where the graph will be registered.
            Not used if client is provided.
        :param client: The IndexifyClient used to communicate with the server.
            Preferred over server_url.
        """
        self._name = name
        if client:
            self._client = client
        else:
            self._client = IndexifyClient(service_url=server_url)

        self._graph_definition: ComputeGraphMetadata = self._client.graph(self._name)

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
            self._name,
            block_until_done,
            self._graph_definition.get_input_encoder(),
            **kwargs
        )

    def metadata(self) -> ComputeGraphMetadata:
        """
        Get the metadata of the graph.
        """
        return self._client.graph(self._name)

    def replay_invocations(self):
        """
        Replay all the graph previous runs/invocations on the latest version of the graph.

        This is useful to make all the previous invocations go through
        an updated graph to take advantage of graph improvements.
        """
        self._client.replay_invocations(self._name)

    @classmethod
    def deploy(
        cls,
        g: Graph,
        additional_modules=[],
        server_url: Optional[str] = DEFAULT_SERVICE_URL,
        client: Optional[IndexifyClient] = None,
    ):
        """
        Create a new RemoteGraph from a local Graph object.

        :param g: The local Graph object.
        :param additional_modules: List of additional modules to be registered with the graph.
            Needed for modules that are imported outside of an indexify function.
        :param server_url: The URL of the server where the graph will be registered.
            Not used if client is provided.
        :param client: The IndexifyClient used to communicate with the server.
            Preferred over server_url.
        """
        g.validate_graph()
        if not client:
            client = IndexifyClient(service_url=server_url)
        client.register_compute_graph(g, additional_modules)
        return cls(name=g.name, server_url=server_url, client=client)

    @classmethod
    def by_name(
        cls,
        name: str,
        server_url: Optional[str] = DEFAULT_SERVICE_URL,
        client: Optional[IndexifyClient] = None,
    ):
        """
        Create a handle to call a RemoteGraph by name.

        :param name: The name of the graph.
        :param server_url: The URL of the server where the graph will be registered.
            Not used if client is provided.
        :param client: The IndexifyClient used to communicate with the server.
            Preferred over server_url.
        :return: A RemoteGraph object.
        """
        return cls(name=name, server_url=server_url, client=client)

    def output(
        self,
        invocation_id: str,
        fn_name: str,
    ) -> List[Any]:
        """
        Returns the extracted objects by a graph for an ingested object.

        - If the extractor name is provided, only the objects extracted by that extractor are returned.
        - If the extractor name is not provided, all the extracted objects are returned for the input object.

        :param invocation_id (str): The ID of the ingested object
        :param fn_name (Optional[str]): The name of the function whose output is to be returned if provided
        :return (List[Any]): Output of the function.
        """

        return self._client.graph_outputs(
            graph=self._name,
            invocation_id=invocation_id,
            fn_name=fn_name,
        )
