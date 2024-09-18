from typing import Any, List, Optional, Type

import cbor2
from pydantic import BaseModel

from .data_objects import IndexifyData


class CborSerializer:
    @staticmethod
    def serialize(data: Any) -> bytes:
        if (
            isinstance(data, type)
            and issubclass(data, BaseModel)
            or isinstance(data, BaseModel)
        ):
            return cbor2.dumps(data.model_dump())
        return cbor2.dumps(data)

    @staticmethod
    def deserialize(data: bytes) -> IndexifyData:
        cached_output = cbor2.loads(data)
        return IndexifyData(**cached_output)
