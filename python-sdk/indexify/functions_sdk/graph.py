import inspect
from collections import defaultdict
from inspect import signature
from operator import index
import re
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

import cbor2
import cloudpickle
from pydantic import BaseModel
from typing_extensions import get_args, get_origin

from .cbor_serializer import CborSerializer
from .data_objects import BaseData, RouterOutput
from .indexify_functions import (
    IndexifyFunction,
    IndexifyFunctionWrapper,
    IndexifyRouter,
)

RouterFn = Annotated[Callable[[BaseData], Optional[List[IndexifyFunction]]], "RouterFn"]
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


class RouterMetadata(BaseModel):
    name: str
    description: str
    source_fn: str
    target_fns: List[str]


class NodeMetadata(BaseModel):
    dynamic_router: Optional[RouterMetadata] = None
    compute_fn: Optional[FunctionMetadata] = None


class ComputeGraphMetadata(BaseModel):
    name: str
    description: str
    start_node: NodeMetadata
    nodes: Dict[str, NodeMetadata]
    edges: Dict[str, List[str]]


class Graph:
    def __init__(
        self, name: str, start_node: IndexifyFunction, description: Optional[str] = None
    ):
        self.name = name
        self.description = description
        self.nodes: Dict[str, Union[IndexifyFunction, IndexifyRouter]] = {}
        self.routers: Dict[str, List[str]] = defaultdict(list)
        self.edges: Dict[str, List[str]] = defaultdict(list)

        self._start_node: str = start_node
        self.add_node(start_node)

    def get_function(self, name: str) -> IndexifyFunctionWrapper:
        if name not in self.nodes:
            raise ValueError(f"Function {name} not found in graph")
        return IndexifyFunctionWrapper(self.nodes[name])

    def add_node(
        self, indexify_fn: Union[Type[IndexifyFunction], Type[IndexifyRouter]]
    ) -> "Graph":
        # Validation
        if not (
            indexify_fn.__name__ == "IndexifyFn" or
            indexify_fn.__name__ == "IndexifyRo"
        ):
            raise Exception(
                f"Unable to add node of type `{indexify_fn.__name__}`. "
                f"Required, `IndexifyFunction` or `IndexifyRouter`"
            )

        signature = inspect.signature(indexify_fn.run)

        for param in signature.parameters.values():
            if param.annotation == inspect.Parameter.empty:
                raise Exception(
                    f"Input param {param.name} in {indexify_fn.name} has empty"
                    f" type annotation"
                )

        if signature.return_annotation == inspect.Signature.empty:
            raise Exception(
                f"Function {indexify_fn.name} has empty return type annotation"
            )

        # main
        if indexify_fn.name in self.nodes:
            return self

        self.nodes[indexify_fn.name] = indexify_fn
        return self

    def route(
        self, from_node: Type[IndexifyRouter], to_nodes: List[Type[IndexifyFunction]]
    ) -> "Graph":
        # Validation
        print(from_node)
        signature = inspect.signature(from_node.run)

        if signature.return_annotation == inspect.Signature.empty:
            raise Exception(
                f"Function {from_node.name} has empty return type annotation"
            )

        # We lose the exact type string when the object is created
        source = inspect.getsource(from_node.run)
        pattern = r'Union\[((?:\w+(?:,\s*)?)+)\]'
        match = re.search(pattern, source)
        src_route_nodes = None
        if match:
            # nodes = re.findall(r'\w+', match.group(1))
            src_route_nodes = [node.strip() for node in match.group(1).split(',')]
        else:
            raise Exception(
                f"Invalid router for {from_node.name}, cannot find output nodes"
            )

        to_node_names = [i.name for i in to_nodes]

        for src_node in src_route_nodes:
            if src_node not in to_node_names:
                raise Exception(
                    f"Unable to find {src_node} in to_nodes "
                    f"{to_node_names}"
                )

        # main
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

    def invoke_fn_ser(self, name: str, serialized_input: bytes) -> List[bytes]:
        fn_wrapper = self.get_function(name)
        input = cbor2.loads(serialized_input)
        input = self.deserialize_input_from_dict(name, input)
        output: List[BaseData] = fn_wrapper.run(input.payload)
        return CborSerializer.serialize(output)

    def invoke_router(
        self, name: str, serialized_input: bytes
    ) -> Optional[RouterOutput]:
        fn_wrapper = self.get_function(name)
        input = cbor2.loads(serialized_input)
        input = self.deserialize_input_from_dict(name, input)
        return fn_wrapper.run(input.payload)

    def deserialize_input_from_dict(
        self, compute_fn: str, json_data: Dict[str, Any]
    ) -> BaseData:
        compute_fn = self.nodes[compute_fn]
        if not compute_fn:
            raise ValueError(f"Compute function {compute_fn} not found in graph")
        signature = inspect.signature(compute_fn.run)
        arg_types = {
            name: param.annotation
            if param.annotation != inspect.Parameter.empty
            else None
            for name, param in signature.parameters.items()
        }
        if len(arg_types) > 1:
            raise ValueError(
                f"Compute function {compute_fn} has multiple arguments, but only one is supported"
            )
        elif len(arg_types) == 0:
            raise ValueError(f"Compute function {compute_fn} has no arguments")
        arg_name, arg_type = next(iter(arg_types.items()))
        if arg_type is None:
            raise ValueError(f"Argument {arg_name} has no type annotation")
        payload = json_data.get("payload")
        if payload and is_pydantic_model_from_annotation(arg_type):
            payload_model = arg_type.model_validate(payload)
            json_data["payload"] = payload_model
        elif is_pydantic_model_from_annotation(arg_type):
            if len(json_data.keys()) == 1 and isinstance(
                list(json_data.values())[0], dict
            ):
                json_data = list(json_data.values())[0]
            json_data = {"payload": arg_type.model_validate(json_data)}
        else:
            data_copy = json_data.copy()
            json_data["payload"] = data_copy
        return BaseData(**json_data)

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
        start_node = FunctionMetadata(
            name=self._start_node.name,
            fn_name=self._start_node.fn_name,
            description=self._start_node.description,
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
                    )
                )
            else:
                metadata_nodes[node_name] = NodeMetadata(
                    compute_fn=FunctionMetadata(
                        name=node_name,
                        fn_name=node.fn_name,
                        description=node.description,
                    )
                )
        return ComputeGraphMetadata(
            name=self.name,
            description=self.description or "",
            start_node=NodeMetadata(compute_fn=start_node),
            nodes=metadata_nodes,
            edges=metadata_edges,
        )
