import sys
from collections import defaultdict
from queue import deque
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)

import cloudpickle
from nanoid import generate
from pydantic import BaseModel
from typing_extensions import get_args, get_origin

from .data_objects import IndexifyData, RouterOutput
from .graph_definition import (
    ComputeGraphMetadata,
    FunctionMetadata,
    NodeMetadata,
    RouterMetadata,
    RuntimeInformation,
)
from .graph_validation import validate_node, validate_route
from .indexify_functions import (
    IndexifyFunction,
    IndexifyFunctionWrapper,
    IndexifyRouter,
)
from .local_cache import CacheAwareFunctionWrapper
from .object_serializer import get_serializer

RouterFn = Annotated[
    Callable[[IndexifyData], Optional[List[IndexifyFunction]]], "RouterFn"
]
GraphNode = Annotated[Union[IndexifyFunctionWrapper, RouterFn], "GraphNode"]


def is_pydantic_model_from_annotation(type_annotation):
    if isinstance(type_annotation, str):
        class_name = type_annotation.split("'")[-2].split(".")[-1]
        return False  # Default to False if we can't evaluate

    origin = get_origin(type_annotation)
    if origin is not None:
        args = get_args(type_annotation)
        if args:
            return is_pydantic_model_from_annotation(args[0])

    if isinstance(type_annotation, type):
        return issubclass(type_annotation, BaseModel)

    return False


class Graph:
    def __init__(
        self, name: str, start_node: IndexifyFunction, description: Optional[str] = None
    ):
        self.name = name
        self.description = description
        self.nodes: Dict[str, Union[IndexifyFunction, IndexifyRouter]] = {}
        self.routers: Dict[str, List[str]] = defaultdict(list)
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.accumulator_zero_values: Dict[str, Any] = {}

        self.add_node(start_node)
        self._start_node: str = start_node.name

        # Storage for local execution
        self._results: Dict[str, Dict[str, List[IndexifyData]]] = {}
        self._cache = CacheAwareFunctionWrapper("./indexify_local_runner_cache")
        self._accumulator_values: Dict[str, Dict[str, IndexifyData]] = {}

    def get_function(self, name: str) -> IndexifyFunctionWrapper:
        if name not in self.nodes:
            raise ValueError(f"Function {name} not found in graph")
        return IndexifyFunctionWrapper(self.nodes[name])

    def get_accumulators(self) -> Dict[str, Any]:
        return self.accumulator_zero_values

    def add_node(
        self, indexify_fn: Union[Type[IndexifyFunction], Type[IndexifyRouter]]
    ) -> "Graph":
        validate_node(indexify_fn=indexify_fn)

        if indexify_fn.name in self.nodes:
            return self

        if issubclass(indexify_fn, IndexifyFunction) and indexify_fn.accumulate:
            self.accumulator_zero_values[
                indexify_fn.name
            ] = indexify_fn.accumulate().model_dump()

        self.nodes[indexify_fn.name] = indexify_fn
        return self

    def route(
        self, from_node: Type[IndexifyRouter], to_nodes: List[Type[IndexifyFunction]]
    ) -> "Graph":

        validate_route(from_node=from_node, to_nodes=to_nodes)

        print(
            f"Adding router {from_node.name} to nodes {[node.name for node in to_nodes]}"
        )
        self.add_node(from_node)
        for node in to_nodes:
            self.add_node(node)
            self.routers[from_node.name].append(node.name)
        return self

    def serialize(self, additional_modules):
        # Get all unique modules from nodes and edges
        pickled_functions = {}
        for module in additional_modules:
            cloudpickle.register_pickle_by_value(module)
        for node in self.nodes.values():
            cloudpickle.register_pickle_by_value(sys.modules[node.__module__])
            pickled_functions[node.name] = cloudpickle.dumps(node)
            if not sys.modules[node.__module__] in additional_modules:
                cloudpickle.unregister_pickle_by_value(sys.modules[node.__module__])
        return pickled_functions

    def add_edge(
        self,
        from_node: Type[IndexifyFunction],
        to_node: Union[Type[IndexifyFunction], RouterFn],
    ) -> "Graph":
        self.add_edges(from_node, [to_node])
        return self

    def add_edges(
        self,
        from_node: Union[Type[IndexifyFunction], Type[IndexifyRouter]],
        to_node: List[Union[Type[IndexifyFunction], Type[IndexifyRouter]]],
    ) -> "Graph":
        self.add_node(from_node)
        from_node_name = from_node.name
        for node in to_node:
            self.add_node(node)
            self.edges[from_node_name].append(node.name)
        return self

    def definition(self) -> ComputeGraphMetadata:
        start_node = self.nodes[self._start_node]
        start_node = FunctionMetadata(
            name=start_node.name,
            fn_name=start_node.fn_name,
            description=start_node.description,
            reducer=start_node.accumulate is not None,
            image_name=start_node.image._image_name,
        )
        metadata_edges = self.edges.copy()
        metadata_nodes = {}
        for node_name, node in self.nodes.items():
            if node_name in self.routers:
                metadata_nodes[node_name] = NodeMetadata(
                    dynamic_router=RouterMetadata(
                        name=node_name,
                        description=node.description or "",
                        source_fn=node_name,
                        target_fns=self.routers[node_name],
                        payload_encoder=node.payload_encoder,
                        image_name=node.image._image_name,
                    )
                )
            else:
                metadata_nodes[node_name] = NodeMetadata(
                    compute_fn=FunctionMetadata(
                        name=node_name,
                        fn_name=node.name,
                        description=node.description,
                        reducer=node.accumulate is not None,
                        image_name=node.image._image_name,
                    )
                )

        return ComputeGraphMetadata(
            name=self.name,
            description=self.description or "",
            start_node=NodeMetadata(compute_fn=start_node),
            nodes=metadata_nodes,
            edges=metadata_edges,
            runtime_information=RuntimeInformation(
                major_version=sys.version_info.major,
                minor_version=sys.version_info.minor,
            ),
        )

    def run(self, block_until_done: bool = False, **kwargs) -> str:
        start_node = self.nodes[self._start_node]
        serializer = get_serializer(start_node.payload_encoder)
        input = IndexifyData(id=generate(), payload=serializer.serialize(kwargs))
        print(f"[bold] Invoking {self._start_node}[/bold]")
        outputs = defaultdict(list)
        self._accumulator_values[input.id] = {}
        for k, v in self.accumulator_zero_values.items():
            node = self.nodes[k]
            serializer = get_serializer(node.payload_encoder)
            self._accumulator_values[input.id] = {
                k: IndexifyData(payload=serializer.serialize(v))
            }
        self._results[input.id] = outputs
        self._run(input, outputs)
        return input.id

    def _run(
        self,
        initial_input: IndexifyData,
        outputs: Dict[str, List[bytes]],
    ):
        accumulator_values = self._accumulator_values[initial_input.id]
        queue = deque([(self._start_node, initial_input)])
        while queue:
            node_name, input = queue.popleft()
            node = self.nodes[node_name]
            serializer = get_serializer(node.payload_encoder)
            input_bytes = serializer.serialize(input)
            cached_output_bytes: Optional[bytes] = self._cache.get(
                self.name, node_name, input_bytes
            )
            if cached_output_bytes is not None:
                print(
                    f"ran {node_name}: num outputs: {len(cached_output_bytes)} (cache hit)"
                )
                function_outputs: List[IndexifyData] = []
                cached_output_list = serializer.deserialize_list(cached_output_bytes)
                if accumulator_values.get(node_name, None) is not None:
                    accumulator_values[node_name] = cached_output_list[-1].model_copy()
                    outputs[node_name] = []
                function_outputs.extend(cached_output_list)
                outputs[node_name].extend(cached_output_list)
            else:
                function_outputs: List[IndexifyData] = IndexifyFunctionWrapper(
                    node
                ).invoke_fn_ser(
                    node_name, input, accumulator_values.get(node_name, None)
                )
                print(f"ran {node_name}: num outputs: {len(function_outputs)}")
                if accumulator_values.get(node_name, None) is not None:
                    accumulator_values[node_name] = function_outputs[-1].model_copy()
                    outputs[node_name] = []
                outputs[node_name].extend(function_outputs)
                function_outputs_bytes: List[bytes] = [
                    serializer.serialize_list(function_outputs)
                ]
                self._cache.set(
                    self.name,
                    node_name,
                    input_bytes,
                    function_outputs_bytes,
                )
            if accumulator_values.get(node_name, None) is not None and queue:
                print(
                    f"accumulator not none for {node_name}, continuing, len queue: {len(queue)}"
                )
                continue

            out_edges = self.edges.get(node_name, [])
            # Figure out if there are any routers for this node
            for i, edge in enumerate(out_edges):
                if edge in self.routers:
                    out_edges.remove(edge)
                    for output in function_outputs:
                        dynamic_edges = self._route(edge, output) or []
                        for dynamic_edge in dynamic_edges.edges:
                            if dynamic_edge in self.nodes:
                                print(
                                    f"[bold]dynamic router returned node: {dynamic_edge}[/bold]"
                                )
                                out_edges.append(dynamic_edge)
            for out_edge in out_edges:
                for output in function_outputs:
                    queue.append((out_edge, output))

    def _route(self, node_name: str, input: IndexifyData) -> Optional[RouterOutput]:
        router = self.nodes[node_name]
        return IndexifyFunctionWrapper(router).invoke_router(node_name, input)

    def output(
        self,
        invocation_id: str,
        fn_name: str,
    ) -> List[Any]:
        results = self._results[invocation_id]
        if fn_name not in results:
            raise ValueError(f"no results found for fn {fn_name} on graph {self.name}")
        fn = self.nodes[fn_name]
        fn_model = self.get_function(fn_name).get_output_model()
        serializer = get_serializer(fn.payload_encoder)
        outputs = []
        for result in results[fn_name]:
            payload_dict = serializer.deserialize(result.payload)
            if issubclass(fn_model, BaseModel) and isinstance(payload_dict, dict):
                payload = fn_model.model_validate(payload_dict)
            else:
                payload = payload_dict
            outputs.append(payload)
        return outputs
