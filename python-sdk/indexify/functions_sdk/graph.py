import inspect
from collections import defaultdict
from typing import (
    Annotated,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Type,
    Union,
    get_args,
    get_origin,
)

import cloudpickle
import msgpack
from pydantic import BaseModel
from typing_extensions import get_args, get_origin

from .data_objects import IndexifyData, RouterOutput
from .graph_validation import validate_node, validate_route
from .indexify_functions import (
    IndexifyFunction,
    IndexifyFunctionWrapper,
    IndexifyRouter,
)
from .object_serializer import CloudPickleSerializer, get_serializer

RouterFn = Annotated[
    Callable[[IndexifyData], Optional[List[IndexifyFunction]]], "RouterFn"
]
GraphNode = Annotated[Union[IndexifyFunctionWrapper, RouterFn], "GraphNode"]


def is_pydantic_model_from_annotation(type_annotation):
    # If it's a string representation
    if isinstance(type_annotation, str):
        # Extract the class name from the string
        class_name = type_annotation.split("'")[-2].split(".")[-1]
        # This part is tricky and might require additional context or imports
        # You might need to import the actual class or module where it's defined
        # For example:
        # from indexify.functions_sdk.data_objects import File
        # return issubclass(eval(class_name), BaseModel)
        return False  # Default to False if we can't evaluate

    # If it's a Type object
    origin = get_origin(type_annotation)
    if origin is not None:
        # Handle generic types like List[File], Optional[File], etc.
        args = get_args(type_annotation)
        if args:
            return is_pydantic_model_from_annotation(args[0])

    # If it's a direct class reference
    if isinstance(type_annotation, type):
        return issubclass(type_annotation, BaseModel)

    return False


class FunctionMetadata(BaseModel):
    name: str
    fn_name: str
    description: str
    reducer: bool = False
    image_name: str
    payload_encoder: str = "cloudpickle"


class RouterMetadata(BaseModel):
    name: str
    description: str
    source_fn: str
    target_fns: List[str]
    image_name: str
    payload_encoder: str = "cloudpickle"


class NodeMetadata(BaseModel):
    dynamic_router: Optional[RouterMetadata] = None
    compute_fn: Optional[FunctionMetadata] = None


class ComputeGraphMetadata(BaseModel):
    name: str
    description: str
    start_node: NodeMetadata
    nodes: Dict[str, NodeMetadata]
    edges: Dict[str, List[str]]
    accumulator_zero_values: Dict[str, bytes] = {}

    def get_input_payload_serializer(self):
        return get_serializer(self.start_node.compute_fn.payload_encoder)


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

    def get_function(self, name: str) -> IndexifyFunctionWrapper:
        if name not in self.nodes:
            raise ValueError(f"Function {name} not found in graph")
        return IndexifyFunctionWrapper(self.nodes[name])

    def get_accumulators(self) -> Dict[str, Any]:
        return self.accumulator_zero_values

    def deserialize_fn_output(self, name: str, output: IndexifyData) -> Any:
        serializer = get_serializer(self.nodes[name].payload_encoder)
        return serializer.deserialize(output.payload)

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

    def serialize(self):
        return cloudpickle.dumps(self)

    @staticmethod
    def deserialize(graph: bytes) -> "Graph":
        return cloudpickle.loads(graph)

    @staticmethod
    def from_path(path: str) -> "Graph":
        with open(path, "rb") as f:
            return cloudpickle.load(f)

    def add_edge(
        self,
        from_node: Type[IndexifyFunction],
        to_node: Union[Type[IndexifyFunction], RouterFn],
    ) -> "Graph":
        self.add_edges(from_node, [to_node])
        return self

    def invoke_fn_ser(
        self, name: str, input: IndexifyData, acc: Optional[Any] = None
    ) -> List[IndexifyData]:
        fn_wrapper = self.get_function(name)
        input = self.deserialize_input(name, input)
        serializer = get_serializer(fn_wrapper.indexify_function.payload_encoder)
        if acc is not None:
            acc = fn_wrapper.indexify_function.accumulate.model_validate(
                serializer.deserialize(acc.payload)
            )
        if acc is None and fn_wrapper.indexify_function.accumulate is not None:
            acc = fn_wrapper.indexify_function.accumulate.model_validate(
                self.accumulator_zero_values[name]
            )
        outputs: List[Any] = fn_wrapper.run_fn(input, acc=acc)
        return [
            IndexifyData(payload=serializer.serialize(output)) for output in outputs
        ]

    def invoke_router(self, name: str, input: IndexifyData) -> Optional[RouterOutput]:
        fn_wrapper = self.get_function(name)
        input = self.deserialize_input(name, input)
        return RouterOutput(edges=fn_wrapper.run_router(input))

    def deserialize_input(self, compute_fn: str, indexify_data: IndexifyData) -> Any:
        compute_fn = self.nodes[compute_fn]
        if not compute_fn:
            raise ValueError(f"Compute function {compute_fn} not found in graph")
        if compute_fn.payload_encoder == "cloudpickle":
            return CloudPickleSerializer.deserialize(indexify_data.payload)
        payload = msgpack.unpackb(indexify_data.payload)
        signature = inspect.signature(compute_fn.run)
        arg_types = {}
        for name, param in signature.parameters.items():
            if (
                param.annotation != inspect.Parameter.empty
                and param.annotation != getattr(compute_fn, "accumulate", None)
            ):
                arg_types[name] = param.annotation
        if len(arg_types) > 1:
            raise ValueError(
                f"Compute function {compute_fn} has multiple arguments, but only one is supported"
            )
        elif len(arg_types) == 0:
            raise ValueError(f"Compute function {compute_fn} has no arguments")
        arg_name, arg_type = next(iter(arg_types.items()))
        if arg_type is None:
            raise ValueError(f"Argument {arg_name} has no type annotation")
        if is_pydantic_model_from_annotation(arg_type):
            if len(payload.keys()) == 1 and isinstance(list(payload.values())[0], dict):
                payload = list(payload.values())[0]
            return arg_type.model_validate(payload)
        return payload

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
                        fn_name=node.fn_name,
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
        )
