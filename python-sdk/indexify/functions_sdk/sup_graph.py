from typing import Optional, Type, Union, List, Any

from indexify import LocalClient, RemoteClient
from indexify.error import ApiException
from indexify.functions_sdk.graph import Graph, RouterFn
from indexify.functions_sdk.indexify_functions import IndexifyFunction, \
    IndexifyRouter


class SubGraphRemoteStub:
    name: str
    server_url: str


# class Graph
class SupGraph:
    def __init__(
        self,
        name: str,
        start_node: Optional[IndexifyFunction] = None,
        description: Optional[str] = None,
        server_url: Optional[str] = None,
    ):
        if server_url is None:
            self.client = LocalClient()
        else:
            self.client = RemoteClient(service_url=server_url)

        if start_node is None:
            # When we run a graph remotely we just need to save the name
            self.graph = None
            self.name = name
        else:
            self.graph = Graph(
                name=name, start_node=start_node, description=description
            )

    def add_edge(
        self,
        from_node: Type[IndexifyFunction],
        to_node: Union[Type[IndexifyFunction], RouterFn],
    ) -> "SupGraph":
        self.graph.add_edges(from_node, [to_node])
        return self

    def route(
        self, from_node: Type[IndexifyRouter],
        to_nodes: List[Type[IndexifyFunction]]
    ) -> "SupGraph":
        self.graph.route(from_node, to_nodes)
        return self

    def run(
        self, block_until_done: bool = False, **kwargs
    ) -> str:
        if self.graph is not None:
            self.client.register_compute_graph(self.graph)

        if self.graph is None:
            name = self.name
        else:
            name = self.graph.name

        return self.client.invoke_graph_with_object(
            name, block_until_done, **kwargs
        )

    def register_remote(self):
        if self.graph is not None:
            self.client.register_compute_graph(self.graph)

    def graph_outputs(
        self,
        invocation_id: str,
        fn_name: Optional[str],
    ) -> List[Any]:
        # TODO check this return method (see client interface)
        if self.graph is None:
            name = self.name
        else:
            name = self.graph.name

        return self.client.graph_outputs(
            graph=name,
            invocation_id=invocation_id,
            fn_name=fn_name,
        )

    @staticmethod
    def from_server(
        server_url: str,
        namespace: str,
        name: str
    ) -> 'SupGraph':
        remote_client = RemoteClient(service_url=server_url)

        remote_client.get_graph(namespace=namespace, name=name)

        return SupGraph(name=name, start_node=None, server_url=server_url)
