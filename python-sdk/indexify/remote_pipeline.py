from typing import Optional

from indexify.functions_sdk.pipeline import Pipeline

from .http_client import IndexifyClient
from .remote_graph import RemoteGraph


class RemotePipeline(RemoteGraph):
    @classmethod
    def deploy(
        cls,
        p: Pipeline,
        additional_modules=[],
        server_url: Optional[str] = "http://localhost:8900",
    ):
        """
        Create a new RemoteGraph from a local Graph object.
        :param g: The local Graph object.
        :param server_url: The URL of the server where the graph will be registered.
        """
        cls.graph = p._graph
        client = IndexifyClient(service_url=server_url)
        client.register_compute_graph(p._graph, additional_modules)
        return cls(name=p._graph.name, server_url=server_url)
