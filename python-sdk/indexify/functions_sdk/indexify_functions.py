import inspect
import traceback
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

from pydantic import BaseModel, Field, PrivateAttr

from .data_objects import IndexifyData
from .image import DEFAULT_IMAGE_3_10, Image
from .object_serializer import get_serializer


class GraphInvocationContext(BaseModel):
    invocation_id: str
    graph_name: str
    graph_version: str
    indexify_client: Optional[Any] = Field(default=None)  # avoids circular import
    _local_state: Dict[str, Any] = PrivateAttr(default_factory=dict)

    def set_state_key(self, key: str, value: Any) -> None:
        if self.indexify_client is None:
            self._local_state[key] = value
            return
        self.indexify_client.set_state_key(
            self.graph_name, self.invocation_id, key, value
        )

    def get_state_key(self, key: str) -> Any:
        if self.indexify_client is None:
            return self._local_state.get(key)
        return self.indexify_client.get_state_key(
            self.graph_name, self.invocation_id, key
        )


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
    encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Union[List[Any], Any]:
        pass

    def partial(self, **kwargs) -> Callable:
        from functools import partial

        return partial(self.run, **kwargs)

    @classmethod
    def deserialize_output(cls, output: IndexifyData) -> Any:
        serializer = get_serializer(cls.encoder)
        return serializer.deserialize(output.payload)


class IndexifyRouter:
    name: str = ""
    description: str = ""
    image: Optional[Image] = DEFAULT_IMAGE_3_10
    placement_constraints: List[PlacementConstraints] = []
    encoder: Optional[str] = "cloudpickle"

    def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
        pass


from inspect import signature

from typing_extensions import get_type_hints


def indexify_router(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE_3_10,
    placement_constraints: List[PlacementConstraints] = [],
    encoder: Optional[str] = "cloudpickle",
):
    def construct(fn):
        # Get function signature using inspect.signature
        fn_sig = signature(fn)
        fn_hints = get_type_hints(fn)

        # Create run method that preserves signature
        def run(self, *args, **kwargs):
            return fn(*args, **kwargs)

        # Apply original signature and annotations to run method
        run.__signature__ = fn_sig
        run.__annotations__ = fn_hints

        attrs = {
            "name": name if name else fn.__name__,
            "description": description
            if description
            else (fn.__doc__ or "").strip().replace("\n", ""),
            "image": image,
            "placement_constraints": placement_constraints,
            "encoder": encoder,
            "run": run,
        }

        return type("IndexifyRouter", (IndexifyRouter,), attrs)

    return construct


def indexify_function(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Optional[Image] = DEFAULT_IMAGE_3_10,
    accumulate: Optional[Type[BaseModel]] = None,
    encoder: Optional[str] = "cloudpickle",
    placement_constraints: List[PlacementConstraints] = [],
):
    def construct(fn):
        # Get function signature using inspect.signature
        fn_sig = signature(fn)
        fn_hints = get_type_hints(fn)

        # Create run method that preserves signature
        def run(self, *args, **kwargs):
            return fn(*args, **kwargs)

        # Apply original signature and annotations to run method
        run.__signature__ = fn_sig
        run.__annotations__ = fn_hints

        attrs = {
            "name": name if name else fn.__name__,
            "description": description
            if description
            else (fn.__doc__ or "").strip().replace("\n", ""),
            "image": image,
            "placement_constraints": placement_constraints,
            "accumulate": accumulate,
            "encoder": encoder,
            "run": run,
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
        self.indexify_function: Union[
            IndexifyFunction, IndexifyRouter
        ] = indexify_function()
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
            return [], traceback.format_exc()
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
        serializer = get_serializer(self.indexify_function.encoder)
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
            IndexifyData(
                payload=serializer.serialize(output),
                encoder=self.indexify_function.encoder,
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
