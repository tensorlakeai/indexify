from typing import Any, List

import cloudpickle
import jsonpickle
import msgpack
from pydantic import BaseModel

from .data_objects import IndexifyData


def get_serializer(serializer_type: str) -> Any:
    if serializer_type == "cloudpickle":
        return CloudPickleSerializer()
    elif serializer_type == "msgpack":
        return MsgPackSerializer()
    elif serializer_type == "json":
        return JsonSerializer()
    raise ValueError(f"Unknown serializer type: {serializer_type}")


class JsonSerializer:
    @staticmethod
    def serialize(data: Any) -> str:
        return jsonpickle.encode(data)

    @staticmethod
    def deserialize(data: str) -> Any:
        return jsonpickle.decode(data)

    @staticmethod
    def serialize_list(data: List[Any]) -> str:
        return jsonpickle.encode(data)

    @staticmethod
    def deserialize_list(data: str) -> List[Any]:
        return jsonpickle.decode(data)


class CloudPickleSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        return cloudpickle.dumps(data)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return cloudpickle.loads(data)

    @staticmethod
    def serialize_list(data: List[Any]) -> bytes:
        return cloudpickle.dumps(data)

    @staticmethod
    def deserialize_list(data: bytes) -> List[Any]:
        return cloudpickle.loads(data)


class MsgPackSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        if (
            isinstance(data, type)
            and issubclass(data, BaseModel)
            or isinstance(data, BaseModel)
        ):
            return msgpack.packb(data.model_dump())
        return msgpack.packb(data)

    @staticmethod
    def deserialize(data: bytes) -> IndexifyData:
        cached_output = msgpack.unpackb(data)
        return IndexifyData(**cached_output)

    @staticmethod
    def serialize_list(data: List[IndexifyData]) -> bytes:
        data = [item.model_dump() for item in data]
        return msgpack.packb(data)

    @staticmethod
    def deserialize_list(data: bytes) -> List[IndexifyData]:
        return [IndexifyData(**item) for item in msgpack.unpackb(data)]
