from typing import Optional

from indexify import IndexifyClient
from indexify.function_executor.protocol import RunFunctionRequest
from indexify.functions_sdk.indexify_functions import (
    GraphInvocationContext,
    IndexifyFunctionWrapper,
)
from indexify.functions_sdk.object_serializer import CloudPickleSerializer


class FunctionLoader:
    def __init__(
        self,
        request: RunFunctionRequest,
        indexify_client: Optional[IndexifyClient],
    ):
        self._request = request
        self._indexify_client = indexify_client

    def load(self) -> IndexifyFunctionWrapper:
        """Loads the function's code and returns wrapper for it.

        The loading might fail with error messages add to stdout or stderr
        by function's code or dependencies.
        """

        graph = CloudPickleSerializer.deserialize(self._request.graph.data)
        context = GraphInvocationContext(
            invocation_id=self._request.graph_invocation_id,
            graph_name=self._request.graph_name,
            graph_version=str(self._request.graph_version),
            indexify_client=self._indexify_client,
        )
        return IndexifyFunctionWrapper(
            CloudPickleSerializer.deserialize(graph[self._request.function_name]),
            context,
        )
