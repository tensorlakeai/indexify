import inspect
import re
import sys
import traceback
from abc import ABC, abstractmethod
from functools import update_wrapper
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import msgpack
from pydantic import BaseModel
from typing_extensions import get_type_hints

from .data_objects import IndexifyData, RouterOutput
from .image import DEFAULT_IMAGE_3_10, Image
from .object_serializer import CloudPickleSerializer, get_serializer


def format_filtered_traceback(exc_info=None):
    """
    Format a traceback excluding indexify_functions.py lines.
    Can be used in exception handlers to replace traceback.format_exc()
    """
    if exc_info is None:
        exc_info = sys.exc_info()

    # Get the full traceback as a string
    full_traceback = traceback.format_exception(*exc_info)

    # Filter out lines containing indexify_functions.py
    filtered_lines = []
    skip_next = False

    for line in full_traceback:
        if "indexify_functions.py" in line:
            skip_next = True
            continue
        if skip_next:
            if line.strip().startswith("File "):
                skip_next = False
            else:
                continue
        filtered_lines.append(line)

    # Clean up any double blank lines that might have been created
    cleaned = re.sub(r"\n\s*\n\s*\n", "\n\n", "".join(filtered_lines))
    return cleaned


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


class PlacementConstraints(BaseModel):
    min_python_version: Optional[str] = "3.9"
    max_python_version: Optional[str] = None
    platform: Optional[str] = None
    image_name: Optional[str] = None


class IndexifyFunction:
    name: str = ""
    description: str = ""
    image: Optional[Image] = DEFAULT_IMAGE_3_10
    placement_constraints: List[PlacementConstraints] = []
    accumulate: Optional[Type[Any]] = None
    payload_encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Union[List[Any], Any]:
        pass

    def partial(self, **kwargs) -> Callable:
        from functools import partial

        return partial(self.run, **kwargs)

    @classmethod
    def deserialize_output(cls, output: IndexifyData) -> Any:
        serializer = get_serializer(cls.payload_encoder)
        return serializer.deserialize(output.payload)


class IndexifyRouter:
    name: str = ""
    description: str = ""
    image: Optional[Image] = DEFAULT_IMAGE_3_10
    placement_constraints: List[PlacementConstraints] = []
    payload_encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
        pass


def indexify_router(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE_3_10,
    placement_constraints: List[PlacementConstraints] = [],
    payload_encoder: Optional[str] = "cloudpickle",
):
    def construct(fn):
        args = locals().copy()
        args["name"] = args["name"] if args.get("name", None) else fn.__name__
        args["fn_name"] = fn.__name__
        args["description"] = (
            args["description"]
            if args.get("description", None)
            else (fn.__doc__ or "").strip().replace("\n", "")
        )

        class IndexifyRo(IndexifyRouter):
            def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
                return fn(*args, **kwargs)

            update_wrapper(run, fn)

        for key, value in args.items():
            if key != "fn" and key != "self":
                setattr(IndexifyRo, key, value)

        IndexifyRo.image = image
        IndexifyRo.payload_encoder = payload_encoder
        return IndexifyRo

    return construct


def indexify_function(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE_3_10,
    accumulate: Optional[Type[BaseModel]] = None,
    payload_encoder: Optional[str] = "cloudpickle",
    placement_constraints: List[PlacementConstraints] = [],
):
    def construct(fn):
        args = locals().copy()
        args["name"] = args["name"] if args.get("name", None) else fn.__name__
        args["fn_name"] = fn.__name__
        args["description"] = (
            args["description"]
            if args.get("description", None)
            else (fn.__doc__ or "").strip().replace("\n", "")
        )

        class IndexifyFn(IndexifyFunction):
            def run(self, *args, **kwargs) -> Union[List[Any], Any]:
                return fn(*args, **kwargs)

            update_wrapper(run, fn)

        for key, value in args.items():
            if key != "fn" and key != "self":
                setattr(IndexifyFn, key, value)

        IndexifyFn.image = image
        IndexifyFn.accumulate = accumulate
        IndexifyFn.payload_encoder = payload_encoder
        return IndexifyFn

    return construct


class FunctionCallResult(BaseModel):
    ser_outputs: List[IndexifyData]
    traceback_msg: Optional[str] = None


class RouterCallResult(BaseModel):
    edges: List[str]
    traceback_msg: Optional[str] = None


class IndexifyFunctionWrapper:
    def __init__(self, indexify_function: Union[IndexifyFunction, IndexifyRouter]):
        self.indexify_function: Union[IndexifyFunction, IndexifyRouter] = (
            indexify_function()
        )

    def get_output_model(self) -> Any:
        if not isinstance(self.indexify_function, IndexifyFunction):
            raise TypeError("Input must be an instance of IndexifyFunction")

        extract_method = self.indexify_function.run
        type_hints = get_type_hints(extract_method)
        return_type = type_hints.get("return", Any)
        if get_origin(return_type) is list:
            return_type = get_args(return_type)[0]
        elif get_origin(return_type) is Union:
            inner_types = get_args(return_type)
            if len(inner_types) == 2 and type(None) in inner_types:
                return_type = (
                    inner_types[0] if inner_types[1] is type(None) else inner_types[1]
                )
        return return_type

    def run_router(
        self, input: Union[Dict, Type[BaseModel]]
    ) -> Tuple[List[str], Optional[str]]:
        kwargs = input if isinstance(input, dict) else {"input": input}
        args = []
        kwargs = {}
        if isinstance(input, dict):
            kwargs = input
        else:
            args.append(input)
        try:
            extracted_data = self.indexify_function.run(*args, **kwargs)
        except Exception as e:
            return [], format_filtered_traceback()
        if not isinstance(extracted_data, list) and extracted_data is not None:
            return [extracted_data.name], None
        edges = []
        for fn in extracted_data or []:
            edges.append(fn.name)
        return edges, None

    def run_fn(
        self, input: Union[Dict, Type[BaseModel]], acc: Type[Any] = None
    ) -> Tuple[List[Any], Optional[str]]:
        args = []
        kwargs = {}
        if acc is not None:
            args.append(acc)
        if isinstance(input, dict):
            kwargs = input
        else:
            args.append(input)

        try:
            extracted_data = self.indexify_function.run(*args, **kwargs)
        except Exception as e:
            return [], format_filtered_traceback()
        if extracted_data is None:
            return [], None

        output = (
            extracted_data if isinstance(extracted_data, list) else [extracted_data]
        )
        return output, None

    def invoke_fn_ser(
        self, name: str, input: IndexifyData, acc: Optional[Any] = None
    ) -> FunctionCallResult:
        input = self.deserialize_input(name, input)
        serializer = get_serializer(self.indexify_function.payload_encoder)
        if acc is not None:
            acc = self.indexify_function.accumulate.model_validate(
                serializer.deserialize(acc.payload)
            )
        if acc is None and self.indexify_function.accumulate is not None:
            acc = self.indexify_function.accumulate.model_validate(
                self.indexify_function.accumulate()
            )
        outputs, err = self.run_fn(input, acc=acc)
        ser_outputs = [
            IndexifyData(payload=serializer.serialize(output)) for output in outputs
        ]
        return FunctionCallResult(ser_outputs=ser_outputs, traceback_msg=err)

    def invoke_router(self, name: str, input: IndexifyData) -> RouterCallResult:
        input = self.deserialize_input(name, input)
        edges, err = self.run_router(input)
        return RouterCallResult(edges=edges, traceback_msg=err)

    def deserialize_input(self, compute_fn: str, indexify_data: IndexifyData) -> Any:
        if self.indexify_function.payload_encoder == "cloudpickle":
            return CloudPickleSerializer.deserialize(indexify_data.payload)
        payload = msgpack.unpackb(indexify_data.payload)
        signature = inspect.signature(self.indexify_function.run)
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
