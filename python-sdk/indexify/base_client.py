from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pydantic import Json

from indexify.functions_sdk.graph import Graph


class IndexifyClient(ABC):

    ### Operational APIs
    @abstractmethod
    def register_compute_graph(self, graph: Graph):
        """
        Register a compute graph.
        graph: Graph: The graph to be registered
        """
        pass

    @abstractmethod
    def graphs(self) -> List[str]:
        """
        Get the graphs.
        return: List[str]: The graphs
        """
        pass

    @abstractmethod
    def namespaces(self) -> List[str]:
        """
        Get the namespaces.
        return: List[str]: The namespaces
        """
        pass

    @abstractmethod
    def create_namespace(self, namespace: str):
        """
        Create a namespace.
        namespace: str: The name of the namespace to be created
        """
        pass

    ### Ingestion APIs
    @abstractmethod
    def invoke_graph_with_object(
        self, graph: str, block_until_done: bool = False, **kwargs
    ) -> str:
        """
        Invokes a graph with an input object.
        graph: str: The name of the graph to invoke
        kwargs: Any: Named arguments to be passed to the graph. Example: url="https://www.google.com", web_page_text="Hello world!"
        return: str: The ID of the ingested object
        """
        pass

    @abstractmethod
    def invoke_graph_with_file(
        self,
        graph: str,
        path: str,
        metadata: Optional[Dict[str, Json]] = None,
        block_until_done: bool = False,
    ) -> str:
        """
        Invokes a graph with an input file. The file's mimetype is appropriately detected.
        graph: str: The name of the graph to invoke
        path: str: The path to the file to be ingested
        return: str: The ID of the ingested object
        """
        pass

    @abstractmethod
    def rerun_graph(self, graph: str):
        """
        Rerun a graph.
        graph: str: The name of the graph to rerun
        """
        pass

    ### Retrieval APIs
    @abstractmethod
    def graph_outputs(
        self,
        graph: str,
        invocation_id: str,
        fn_name: Optional[str],
    ) -> Union[Dict[str, List[Any]], List[Any]]:
        """
        Returns the extracted objects by a graph for an ingested object. If the extractor name is provided, only the objects extracted by that extractor are returned.
        If the extractor name is not provided, all the extracted objects are returned for the input object.
        graph: str: The name of the graph
        ingested_object_id: str: The ID of the ingested object
        extractor_name: Optional[str]: The name of the extractor whose output is to be returned if provided
        block_until_done: bool = True: If True, the method will block until the extraction is done. If False, the method will return immediately.
        return: Union[Dict[str, List[Any]], List[Any]]: The extracted objects. If the extractor name is provided, the output is a list of extracted objects by the extractor. If the extractor name is not provided, the output is a dictionary with the extractor name as the key and the extracted objects as the value. If no objects are found, an empty list is returned.
        """
        pass
