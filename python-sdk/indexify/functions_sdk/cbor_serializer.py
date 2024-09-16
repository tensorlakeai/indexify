from typing import List, Optional, Type

import cbor2
from pydantic import BaseModel

from .data_objects import BaseData


class CborSerializer:
    @staticmethod
    def serialize(data: List[BaseData]) -> List[bytes]:
        return [cbor2.dumps(item.model_dump()) for item in data]

    @staticmethod
    def deserialize(data: bytes, model: Type[BaseData]) -> BaseData:
        cached_output = cbor2.loads(data)

        class BaseData(BaseModel):
            content_id: Optional[str] = None
            payload: model = None
            md5_payload_checksum: Optional[str] = None

        output = BaseData.model_validate(cached_output)
        return output
