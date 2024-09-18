import json
import os
from pathlib import Path
from typing import Any, Dict, List, Mapping, Type, Union, get_args, get_origin
from uuid import uuid4

from pydantic import BaseModel, RootModel

from .data_objects import IndexifyData

CachedOutput = RootModel[List[IndexifyData]]


class OutputSerializer:
    def __init__(self, base_path: str = "./serialized_files"):
        self._base_path = base_path

    def _serialize(self, obj: Any, base_path: str = "./serialized_files") -> Any:
        if isinstance(obj, BaseModel):
            result = {}
            for field_name, field_value in obj:
                result[field_name] = self._serialize(field_value)
            result[
                "__pydantic_model__"
            ] = f"{obj.__class__.__module__}.{obj.__class__.__name__}"
            return result
        elif isinstance(obj, bytes):
            file_name = f"{uuid4().hex}.bin"
            file_path = os.path.join(base_path, file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(obj)
            return {"__bytes_file__": file_path}
        elif isinstance(obj, list):
            return [self._serialize(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._serialize(value) for key, value in obj.items()}
        else:
            return obj

    def _deserialize(self, data: Any, model: Type[BaseModel]) -> Any:
        if isinstance(data, dict):
            if "__bytes_file__" in data:
                with open(data["__bytes_file__"], "rb") as f:
                    return f.read()
            elif "__pydantic_model__" in data:
                model_name = data.pop("__pydantic_model__")
                # Find the correct model for nested structures
                if model_name != f"{model.__module__}.{model.__name__}":
                    for field_type in model.__annotations__.values():
                        origin = get_origin(field_type)
                        args = get_args(field_type)
                        if origin is Union and isinstance(
                            None, args
                        ):  # handle Optional
                            args = [arg for arg in args if arg is not type(None)]
                        if args and issubclass(args[0], BaseModel):
                            if f"{args[0].__module__}.{args[0].__name__}" == model_name:
                                model = args[0]
                                break
                return model(
                    **{k: self._deserialize(v, model) for k, v in data.items()}
                )
            else:
                return {k: self._deserialize(v, model) for k, v in data.items()}
        elif isinstance(data, list):
            # For lists, we need to determine the type of the items
            if model.__annotations__:
                first_field_type = next(iter(model.__annotations__.values()))
                origin = get_origin(first_field_type)
                args = get_args(first_field_type)
                if origin is list and args:
                    item_type = args[0]
                    return [self._deserialize(item, item_type) for item in data]
            return [self._deserialize(item, model) for item in data]
        else:
            return data

    def serialize(self, obj: CachedOutput) -> str:
        normalized_dicts = []
        for item in obj.root:
            normalized_dicts.append(self._serialize(item))

        return json.dumps(normalized_dicts, indent=2)

    def deserialize(self, json_str: str, model: Type[BaseModel]) -> BaseModel:
        data = json.loads(json_str)
        normalized_outputs = []
        for obj in data:
            content_id = obj.get("content_id", None)
            md5_payload_checksum = obj.get("md5_payload_checksum", None)
            payload = obj.get("payload")
            deserialized_payload = self._deserialize(
                payload, model.model_fields["payload"].annotation
            )
            normalized_outputs.append(
                IndexifyData(
                    id=content_id,
                    md5_payload_checksum=md5_payload_checksum,
                    payload=deserialized_payload,
                )
            )
        return normalized_outputs
