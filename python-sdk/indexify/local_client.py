from collections import defaultdict
from queue import deque
from typing import Any, Dict, List, Optional, Type, Union

import cbor2
from nanoid import generate
from pydantic import BaseModel, Json
from rich import print

from indexify.base_client import IndexifyClient
from indexify.functions_sdk.cbor_serializer import CborSerializer
from indexify.functions_sdk.data_objects import (
    File,
    IndexifyData,
    RouterOutput,
)
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.local_cache import CacheAwareFunctionWrapper


# Holds the outputs of a
class ContentTree(BaseModel):
    id: str
    outputs: Dict[str, List[IndexifyData]]


class LocalClient(IndexifyClient):
    def __init__(self, cache_dir: str = "./indexify_local_runner_cache"):
        self._cache_dir = cache_dir
        self._graphs: Dict[str, Graph] = {}
        self._results: Dict[str, Dict[str, List[IndexifyData]]] = {}
        self._cache = CacheAwareFunctionWrapper(self._cache_dir)
        self._accumulators: Dict[str, Dict[str, IndexifyData]] = {}

    def register_compute_graph(self, graph: Graph):
        self._graphs[graph.name] = graph

    def run_from_serialized_code(self, code: bytes, **kwargs):
        g = Graph.deserialize(graph=code)
        self.run(g, **kwargs)

    def run(self, g: Graph, **kwargs):
        input = IndexifyData(id=generate(), payload=cbor2.dumps(kwargs))
        print(f"[bold] Invoking {g._start_node}[/bold]")
        outputs = defaultdict(list)
        self._accumulators = {
            k: IndexifyData(payload=CborSerializer.serialize(v))
            for k, v in g.get_accumulators().items()
        }
        self._results[input.id] = outputs
        self._run(g, input, outputs)
        return input.id

    def _run(
        self,
        g: Graph,
        initial_input: bytes,
        outputs: Dict[str, List[bytes]],
    ):
        queue = deque([(g._start_node.name, initial_input)])
        while queue:
            node_name, input = queue.popleft()
            input_bytes = cbor2.dumps(input.model_dump())
            cached_output_bytes: Optional[List[bytes]] = self._cache.get(
                g.name, node_name, input_bytes
            )
            if cached_output_bytes is not None:
                print(
                    f"ran {node_name}: num outputs: {len(cached_output_bytes)} (cache hit)"
                )
                function_outputs: List[IndexifyData] = []
                for cached_output in cached_output_bytes:
                    output = CborSerializer.deserialize(cached_output)
                    function_outputs.append(output)
                    outputs[node_name].append(output)
            else:
                function_outputs: List[IndexifyData] = g.invoke_fn_ser(
                    node_name, input, self._accumulators.get(node_name, None)
                )
                print(f"ran {node_name}: num outputs: {len(function_outputs)}")
                if self._accumulators.get(node_name, None) is not None:
                    self._accumulators[node_name] = function_outputs[-1].model_copy()
                    outputs[node_name] = []
                outputs[node_name].extend(function_outputs)
                function_outputs_bytes: List[bytes] = [
                    CborSerializer.serialize(function_output)
                    for function_output in function_outputs
                ]
                self._cache.set(
                    g.name,
                    node_name,
                    input_bytes,
                    function_outputs_bytes,
                )
            if self._accumulators.get(node_name, None) is not None and queue:
                print(
                    f"accumulator not none for {node_name}, continuing, len queue: {len(queue)}"
                )
                continue

            out_edges = g.edges.get(node_name, [])
            # Figure out if there are any routers for this node
            for i, edge in enumerate(out_edges):
                if edge in g.routers:
                    out_edges.remove(edge)
                    for output in function_outputs:
                        dynamic_edges = self._route(g, edge, output) or []
                        for dynamic_edge in dynamic_edges.edges:
                            if dynamic_edge in g.nodes:
                                print(
                                    f"[bold]dynamic router returned node: {dynamic_edge}[/bold]"
                                )
                                out_edges.append(dynamic_edge)
            for out_edge in out_edges:
                for output in function_outputs:
                    queue.append((out_edge, output))

    def _route(
        self, g: Graph, node_name: str, input: IndexifyData
    ) -> Optional[RouterOutput]:
        return g.invoke_router(node_name, input)

    def graphs(self) -> str:
        return list(self._graphs.keys())

    def namespaces(self) -> str:
        return "local"

    def create_namespace(self, namespace: str):
        pass

    def invoke_graph_with_object(
        self, graph: str, block_until_done: bool = False, **kwargs
    ) -> str:
        graph = self._graphs[graph]
        for key, value in kwargs.items():
            if isinstance(value, BaseModel):
                kwargs[key] = value.model_dump()

        return self.run(graph, **kwargs)

    def invoke_graph_with_file(
        self, graph: str, path: str, metadata: Optional[Dict[str, Json]] = None
    ) -> str:
        graph = self._graphs[graph]
        with open(path, "rb") as f:
            data = f.read()
            file = File(data, metadata=metadata)
        return self.run(graph, file=file)

    def graph_outputs(
        self,
        graph: str,
        invocation_id: str,
        fn_name: str,
    ) -> Union[Dict[str, List[Any]], List[Any]]:
        if invocation_id not in self._results:
            raise ValueError(f"no results found for graph {graph}")
        if fn_name not in self._results[invocation_id]:
            raise ValueError(f"no results found for fn {fn_name} on graph {graph}")
        results = []
        fn_model = self._graphs[graph].get_function(fn_name).get_output_model()
        for result in self._results[invocation_id][fn_name]:
            payload_dict = cbor2.loads(result.payload)
            if issubclass(fn_model, BaseModel):
                payload = fn_model.model_validate(payload_dict)
            else:
                payload = payload_dict
            results.append(payload)
        return results
