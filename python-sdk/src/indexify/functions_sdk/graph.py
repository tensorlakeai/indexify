import importlib
import re
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
import nanoid
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
    FunctionCallResult,
    GraphInvocationContext,
    IndexifyFunction,
    IndexifyFunctionWrapper,
    IndexifyRouter,
    RouterCallResult,
)
from .invocation_state.local_invocation_state import LocalInvocationState
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
        self,
        name: str,
        start_node: IndexifyFunction,
        description: Optional[str] = None,
        tags: Dict[str, str] = {},
        version: str = nanoid.generate(),  # Update graph on every deployment unless user wants to manage the version manually.
    ):
        _validate_identifier(version, "version")
        _validate_identifier(name, "name")
        self.name = name
        self.description = description
        self.nodes: Dict[str, Union[IndexifyFunction, IndexifyRouter]] = {}
        self.routers: Dict[str, List[str]] = defaultdict(list)
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.accumulator_zero_values: Dict[str, Any] = {}
        self.tags = tags
        self.version = version

        self.add_node(start_node)
        if issubclass(start_node, IndexifyRouter):
            self.routers[start_node.name] = []
        self._start_node: str = start_node.name

        # Storage for local execution
        self._results: Dict[str, Dict[str, List[IndexifyData]]] = {}
        self._accumulator_values: Dict[str, IndexifyData] = {}
        self._local_graph_ctx: Optional[GraphInvocationContext] = None

    def get_function(self, name: str) -> IndexifyFunctionWrapper:
        if name not in self.nodes:
            raise ValueError(f"Function {name} not found in graph")
        return IndexifyFunctionWrapper(self.nodes[name], self._local_graph_ctx)

    def get_accumulators(self) -> Dict[str, Any]:
        return self.accumulator_zero_values

    def add_node(
        self, indexify_fn: Union[Type[IndexifyFunction], Type[IndexifyRouter]]
    ) -> "Graph":
        validate_node(indexify_fn=indexify_fn)

        if indexify_fn.name in self.nodes:
            return self

        if issubclass(indexify_fn, IndexifyFunction) and indexify_fn.accumulate:
            self.accumulator_zero_values[indexify_fn.name] = indexify_fn.accumulate()

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
        if issubclass(from_node, IndexifyRouter):
            raise ValueError(
                "Cannot add edges from a router node, use route method instead"
            )

        self.add_node(from_node)
        from_node_name = from_node.name
        for node in to_node:
            self.add_node(node)
            self.edges[from_node_name].append(node.name)
        return self

    def definition(self) -> ComputeGraphMetadata:
        start_node = self.nodes[self._start_node]
        is_reducer = False
        if hasattr(start_node, "accumulate"):
            is_reducer = start_node.accumulate is not None
        start_node = FunctionMetadata(
            name=start_node.name,
            fn_name=start_node.name,
            description=start_node.description,
            reducer=is_reducer,
            image_information=start_node.image.to_image_information(),
            input_encoder=start_node.input_encoder,
            output_encoder=start_node.output_encoder,
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
                        input_encoder=node.input_encoder,
                        output_encoder=node.output_encoder,
                        image_information=node.image.to_image_information(),
                    )
                )
            else:
                metadata_nodes[node_name] = NodeMetadata(
                    compute_fn=FunctionMetadata(
                        name=node_name,
                        fn_name=node.name,
                        description=node.description,
                        reducer=node.accumulate is not None,
                        image_information=node.image.to_image_information(),
                        input_encoder=node.input_encoder,
                        output_encoder=node.output_encoder,
                    )
                )

        return ComputeGraphMetadata(
            name=self.name,
            description=self.description or "",
            start_node=NodeMetadata(compute_fn=start_node),
            nodes=metadata_nodes,
            edges=metadata_edges,
            tags=self.tags,
            runtime_information=RuntimeInformation(
                major_version=sys.version_info.major,
                minor_version=sys.version_info.minor,
                sdk_version=importlib.metadata.version("indexify-python-sdk"),
            ),
            version=self.version,
        )

    def run(self, block_until_done: bool = False, **kwargs) -> str:
        self.validate_graph()
        start_node = self.nodes[self._start_node]
        serializer = get_serializer(start_node.input_encoder)
        input = IndexifyData(
            id=generate(),
            payload=serializer.serialize(kwargs),
            encoder=start_node.input_encoder,
        )
        print(f"[bold] Invoking {self._start_node}[/bold]")
        outputs = defaultdict(list)
        for k, v in self.accumulator_zero_values.items():
            node = self.nodes[k]
            serializer = get_serializer(node.input_encoder)
            self._accumulator_values[k] = IndexifyData(
                payload=serializer.serialize(v), encoder=node.input_encoder
            )
        self._results[input.id] = outputs
        self._local_graph_ctx = GraphInvocationContext(
            invocation_id=input.id,
            graph_name=self.name,
            graph_version=self.version,
            invocation_state=LocalInvocationState(),
        )
        self._run(input, outputs)
        return input.id

    def validate_graph(self) -> None:
        """
        A method to validate that each node in the graph is
        reachable from start node using BFS.
        """
        total_number_of_nodes = len(self.nodes)
        queue = deque([self._start_node])
        visited = {self._start_node}

        while queue:
            current_node_name = queue.popleft()
            neighbours = (
                self.edges[current_node_name]
                if current_node_name in self.edges
                else (
                    self.routers[current_node_name]
                    if current_node_name in self.routers
                    else []
                )
            )

            for neighbour in neighbours:
                if neighbour in visited:
                    continue
                else:
                    visited.add(neighbour)
                    queue.append(neighbour)

        if total_number_of_nodes != len(visited):
            # all the nodes are not reachable from the start_node.
            raise Exception("Some nodes in the graph are not reachable from start node")

    def _run(
        self,
        initial_input: IndexifyData,
        outputs: Dict[str, List[bytes]],
    ):
        queue = deque([(self._start_node, initial_input)])
        while queue:
            node_name, input = queue.popleft()
            function_outputs: Union[FunctionCallResult, RouterCallResult] = (
                self._invoke_fn(node_name, input)
            )
            self._log_local_exec_tracebacks(function_outputs)
            if isinstance(function_outputs, RouterCallResult):
                for edge in function_outputs.edges:
                    if edge in self.nodes:
                        queue.append((edge, input))
                continue
            out_edges = self.edges.get(node_name, [])
            fn_outputs = function_outputs.ser_outputs
            print(f"ran {node_name}: num outputs: {len(fn_outputs)}")
            if self._accumulator_values.get(node_name, None) is not None:
                acc_output = fn_outputs[-1].copy()
                self._accumulator_values[node_name] = acc_output
                outputs[node_name] = []
            if fn_outputs:
                outputs[node_name].extend(fn_outputs)
            if self._accumulator_values.get(node_name, None) is not None and queue:
                print(
                    f"accumulator not none for {node_name}, continuing, len queue: {len(queue)}"
                )
                continue

            for out_edge in out_edges:
                for output in fn_outputs:
                    queue.append((out_edge, output))

    def _invoke_fn(
        self, node_name: str, input: IndexifyData
    ) -> Optional[Union[RouterCallResult, FunctionCallResult]]:
        node = self.nodes[node_name]
        if node_name in self.routers and len(self.routers[node_name]) > 0:
            result = IndexifyFunctionWrapper(node, self._local_graph_ctx).invoke_router(
                node_name, input
            )
            for dynamic_edge in result.edges:
                if dynamic_edge in self.nodes:
                    print(f"[bold]dynamic router returned node: {dynamic_edge}[/bold]")
            return result

        acc_value = self._accumulator_values.get(node_name, None)
        return IndexifyFunctionWrapper(
            node, context=self._local_graph_ctx
        ).invoke_fn_ser(node_name, input, acc_value)

    def _log_local_exec_tracebacks(
        self, results: Union[FunctionCallResult, RouterCallResult]
    ):
        if results.traceback_msg is not None:
            print(results.traceback_msg)
            import os

            print("exiting local execution due to error")
            os._exit(1)

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
        serializer = get_serializer(fn.output_encoder)
        outputs = []
        for result in results[fn_name]:
            payload_dict = serializer.deserialize(result.payload)
            if issubclass(fn_model, BaseModel) and isinstance(payload_dict, dict):
                payload = fn_model.model_validate(payload_dict)
            else:
                payload = payload_dict
            outputs.append(payload)
        return outputs


def _validate_identifier(value: str, name: str) -> None:
    if len(value) > 200:
        raise ValueError(f"{name} must be at most 200 characters")
    # Following S3 object key naming restrictions.
    if not re.match(r"^[a-zA-Z0-9!_\-.*'()]+$", value):
        raise ValueError(
            f"{name} must only contain alphanumeric characters or ! - _ . * ' ( )"
        )