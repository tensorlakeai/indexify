import inspect
import traceback
from inspect import Parameter
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from pydantic import BaseModel
from typing_extensions import get_type_hints

from .data_objects import IndexifyData
from .image import DEFAULT_IMAGE, Image
from .invocation_state.invocation_state import InvocationState
from .object_serializer import get_serializer


class GraphInvocationContext:
    def __init__(
        self,
        invocation_id: str,
        graph_name: str,
        graph_version: str,
        invocation_state: InvocationState,
    ):
        self.invocation_id = invocation_id
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.invocation_state = invocation_state


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
    image: Optional[Image] = DEFAULT_IMAGE
    placement_constraints: List[PlacementConstraints] = []
    accumulate: Optional[Type[Any]] = None
    input_encoder: Optional[str] = "cloudpickle"
    output_encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Union[List[Any], Any]:
        pass

    def _call_run(self, *args, **kwargs) -> Union[List[Any], Any]:
        # Process dictionary argument mapping it to args or to kwargs.
        if self.accumulate and len(args) == 2 and isinstance(args[1], dict):
            sig = inspect.signature(self.run)
            new_args = [args[0]]  # Keep the accumulate argument
            dict_arg = args[1]
            new_args_from_dict, new_kwargs = _process_dict_arg(dict_arg, sig)
            new_args.extend(new_args_from_dict)
            return self.run(*new_args, **new_kwargs)
        elif len(args) == 1 and isinstance(args[0], dict):
            sig = inspect.signature(self.run)
            dict_arg = args[0]
            new_args, new_kwargs = _process_dict_arg(dict_arg, sig)
            return self.run(*new_args, **new_kwargs)

        return self.run(*args, **kwargs)

    @classmethod
    def deserialize_output(cls, output: IndexifyData) -> Any:
        serializer = get_serializer(cls.output_encoder)
        return serializer.deserialize(output.payload)


class IndexifyRouter:
    name: str = ""
    description: str = ""
    image: Optional[Image] = DEFAULT_IMAGE
    placement_constraints: List[PlacementConstraints] = []
    input_encoder: Optional[str] = "cloudpickle"
    output_encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
        pass

    # Create run method that preserves signature
    def _call_run(self, *args, **kwargs):
        # Process dictionary argument mapping it to args or to kwargs.
        if len(args) == 1 and isinstance(args[0], dict):
            sig = inspect.signature(self.run)
            dict_arg = args[0]
            new_args, new_kwargs = _process_dict_arg(dict_arg, sig)
            return self.run(*new_args, **new_kwargs)

        return self.run(*args, **kwargs)


def _process_dict_arg(dict_arg: dict, sig: inspect.Signature) -> Tuple[list, dict]:
    new_args = []
    new_kwargs = {}
    remaining_kwargs = dict_arg.copy()

    # Match dictionary keys to function parameters
    for param_name, param in sig.parameters.items():
        if param_name in dict_arg:
            new_args.append(dict_arg[param_name])
            remaining_kwargs.pop(param_name, None)

    if any(v.kind == Parameter.VAR_KEYWORD for v in sig.parameters.values()):
        # Combine remaining dict items with additional kwargs
        new_kwargs.update(remaining_kwargs)
    elif len(remaining_kwargs) > 0:
        # If there are remaining kwargs, add them as a single dict argument
        new_args.append(remaining_kwargs)

    return new_args, new_kwargs


def indexify_router(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE,
    placement_constraints: List[PlacementConstraints] = [],
    input_encoder: Optional[str] = "cloudpickle",
    output_encoder: Optional[str] = "cloudpickle",
):
    def construct(fn):
        attrs = {
            "name": name if name else fn.__name__,
            "description": (
                description
                if description
                else (fn.__doc__ or "").strip().replace("\n", "")
            ),
            "image": image,
            "placement_constraints": placement_constraints,
            "input_encoder": input_encoder,
            "output_encoder": output_encoder,
            "run": staticmethod(fn),
        }

        return type("IndexifyRouter", (IndexifyRouter,), attrs)

    return construct


def indexify_function(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE,
    accumulate: Optional[Type[BaseModel]] = None,
    input_encoder: Optional[str] = "cloudpickle",
    output_encoder: Optional[str] = "cloudpickle",
    placement_constraints: List[PlacementConstraints] = [],
):
    def construct(fn):
        attrs = {
            "name": name if name else fn.__name__,
            "description": (
                description
                if description
                else (fn.__doc__ or "").strip().replace("\n", "")
            ),
            "image": image,
            "placement_constraints": placement_constraints,
            "accumulate": accumulate,
            "input_encoder": input_encoder,
            "output_encoder": output_encoder,
            "run": staticmethod(fn),
        }

        return type("IndexifyFunction", (IndexifyFunction,), attrs)

    return construct


class FunctionCallResult(BaseModel):
    ser_outputs: List[IndexifyData]
    traceback_msg: Optional[str] = None


class RouterCallResult(BaseModel):
    edges: List[str]
    traceback_msg: Optional[str] = None


class IndexifyFunctionWrapper:
    def __init__(
        self,
        indexify_function: Union[IndexifyFunction, IndexifyRouter],
        context: GraphInvocationContext,
    ):
        self.indexify_function: Union[IndexifyFunction, IndexifyRouter] = (
            indexify_function()
        )
        self.indexify_function._ctx = context

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

    def get_input_types(self) -> Dict[str, Any]:
        if not isinstance(self.indexify_function, IndexifyFunction):
            raise TypeError("Input must be an instance of IndexifyFunction")

        extract_method = self.indexify_function.run
        type_hints = get_type_hints(extract_method)
        return {
            k: v
            for k, v in type_hints.items()
            if k != "return" and not is_pydantic_model_from_annotation(v)
        }

    def run_router(
        self, input: Union[Dict, Type[BaseModel]]
    ) -> Tuple[List[str], Optional[str]]:
        args = []
        kwargs = {}
        try:
            # tuple and list are considered positional arguments, list is used for compatibility
            # with json encoding which won't deserialize in tuple.
            if isinstance(input, tuple) or isinstance(input, list):
                args += input
            else:
                args.append(input)
            extracted_data = self.indexify_function._call_run(*args, **kwargs)
        except Exception as e:
            return [], traceback.format_exc()
        if not isinstance(extracted_data, list) and extracted_data is not None:
            return [extracted_data.name], None
        edges = []
        for fn in extracted_data or []:
            edges.append(fn.name)
        return edges, None

    def run_fn(
        self, input: Union[Dict, Type[BaseModel], List, Tuple], acc: Type[Any] = None
    ) -> Tuple[List[Any], Optional[str]]:
        args = []
        kwargs = {}

        if acc is not None:
            args.append(acc)

        # tuple and list are considered positional arguments, list is used for compatibility
        # with json encoding which won't deserialize in tuple.
        if isinstance(input, tuple) or isinstance(input, list):
            args += input
        else:
            args.append(input)

        try:
            extracted_data = self.indexify_function._call_run(*args, **kwargs)
        except Exception as e:
            return [], traceback.format_exc()
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
        input_serializer = get_serializer(self.indexify_function.input_encoder)
        output_serializer = get_serializer(self.indexify_function.output_encoder)
        if acc is not None:
            acc = input_serializer.deserialize(acc.payload)
        if acc is None and self.indexify_function.accumulate is not None:
            acc = self.indexify_function.accumulate()
        outputs, err = self.run_fn(input, acc=acc)
        ser_outputs = [
            IndexifyData(
                payload=output_serializer.serialize(output),
                encoder=self.indexify_function.output_encoder,
            )
            for output in outputs
        ]
        return FunctionCallResult(ser_outputs=ser_outputs, traceback_msg=err)

    def invoke_router(self, name: str, input: IndexifyData) -> RouterCallResult:
        input = self.deserialize_input(name, input)
        edges, err = self.run_router(input)
        return RouterCallResult(edges=edges, traceback_msg=err)

    def deserialize_input(self, compute_fn: str, indexify_data: IndexifyData) -> Any:
        encoder = indexify_data.encoder
        payload = indexify_data.payload
        serializer = get_serializer(encoder)
        return serializer.deserialize(payload)


def get_ctx() -> GraphInvocationContext:
    frame = inspect.currentframe()
    caller_frame = frame.f_back.f_back
    function_instance = caller_frame.f_locals["self"]
    del frame
    del caller_frame
    if isinstance(function_instance, IndexifyFunctionWrapper):
        return function_instance.indexify_function._ctx
    return function_instance._ctx
