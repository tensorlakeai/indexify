from abc import ABC, abstractmethod
from functools import update_wrapper
from typing import (
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

from pydantic import BaseModel
from typing_extensions import get_type_hints

from .data_objects import BaseData, RouterOutput
from .image import Image


class EmbeddingIndexes(BaseModel):
    dim: int
    distance: Optional[str] = "cosine"
    database_url: Optional[str] = None


class PlacementConstraints(BaseModel):
    min_python_version: Optional[str] = "3.9"
    max_python_version: Optional[str] = None
    platform: Optional[str] = None
    image_name: Optional[str] = None


class IndexifyFunction(ABC):
    name: str = ""
    base_image: Optional[str] = None
    description: str = ""
    placement_constraints: List[PlacementConstraints] = []

    @abstractmethod
    def run(self, *args, **kwargs) -> Union[List[Any], Any]:
        pass

    def partial(self, **kwargs) -> Callable:
        from functools import partial

        return partial(self.run, **kwargs)


class IndexifyRouter(ABC):
    name: str = ""
    description: str = ""
    image: Image = None
    placement_constraints: List[PlacementConstraints] = []

    @abstractmethod
    def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
        pass


def indexify_router(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Image = None,
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

        class IndexifyRo(IndexifyRouter):
            def run(self, *args, **kwargs) -> Optional[List[IndexifyFunction]]:
                return fn(*args, **kwargs)

            update_wrapper(run, fn)

        for key, value in args.items():
            if key != "fn" and key != "self":
                setattr(IndexifyRo, key, value)

        IndexifyRo.image = image

        return IndexifyRo

    return construct


def indexify_function(
    name: Optional[str] = None,
    description: Optional[str] = "",
    image: Image = None,
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

        return IndexifyFn

    return construct


class IndexifyFunctionWrapper:
    def __init__(self, indexify_function: Union[IndexifyFunction, IndexifyRouter]):
        self.indexify_function: Union[
            IndexifyFunction, IndexifyRouter
        ] = indexify_function()

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

    def run(
        self, input: Union[Dict, Type[BaseModel]]
    ) -> Union[List[BaseData], RouterOutput]:
        if isinstance(input, dict):
            extracted_data = self.indexify_function.run(**input)
        else:
            extracted_data = self.indexify_function.run(input)

        if isinstance(self.indexify_function, IndexifyRouter):
            if not isinstance(extracted_data, list) and extracted_data is not None:
                return RouterOutput(edges=[extracted_data.name])
            edges = []
            for fn in extracted_data or []:
                edges.append(fn.name)
            return RouterOutput(edges=edges)

        if not isinstance(extracted_data, list):
            return [BaseData(payload=extracted_data)]

        outputs = []
        for data in extracted_data:
            outputs.append(BaseData(payload=data))
        return outputs
