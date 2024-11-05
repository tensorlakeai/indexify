from typing import Union

from indexify.functions_sdk.indexify_functions import (
    IndexifyFunction,
    IndexifyRouter,
)

from .graph import Graph


class Pipeline:
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._graph: Graph = None
        self._last_step = None

    def add_step(self, function: Union[IndexifyFunction, IndexifyRouter]):
        if self._graph is None:
            self._graph = Graph(
                name=self.name, description=self.description, start_node=function
            )
            self._last_step = function
            return
        self._graph.add_edge(self._last_step, function)
        self._last_step = function

    def run(self, **kwargs):
        invocation_id = self._graph.run(**kwargs)
        return invocation_id

    def output(self, invocation_id: str, function_name: str):
        return self._graph.output(invocation_id, function_name)
