from typing import Any, List

import msgpack
from pydantic import BaseModel

from .data_objects import IndexifyData


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
        print(cached_output)
        return IndexifyData(**cached_output)

    @staticmethod
    def serialize_list(data: List[IndexifyData]) -> bytes:
        data = [item.model_dump() for item in data]
        return msgpack.packb(data)

    @staticmethod
    def deserialize_list(data: bytes) -> List[IndexifyData]:
        return [IndexifyData(**item) for item in msgpack.unpackb(data)]
