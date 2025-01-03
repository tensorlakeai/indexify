import json
from typing import Any, List, Type

import cloudpickle


def get_serializer(serializer_type: str) -> Any:
    if serializer_type == "cloudpickle":
        return CloudPickleSerializer()
    elif serializer_type == "json":
        return JsonSerializer()
    elif serializer_type == JsonSerializer.content_type:
        return JsonSerializer()
    elif serializer_type == CloudPickleSerializer.content_type:
        return CloudPickleSerializer()
    raise ValueError(f"Unknown serializer type: {serializer_type}")


class JsonSerializer:
    content_type = "application/json"
    encoding_type = "json"

    @staticmethod
    def serialize(data: Any) -> str:
        try:
            return json.dumps(data)
        except Exception as e:
            raise ValueError(f"failed to serialize data with json: {e}")

    @staticmethod
    def deserialize(data: str) -> Any:
        try:
            if isinstance(data, bytes):
                data = data.decode("utf-8")
            return json.loads(data)
        except Exception as e:
            raise ValueError(f"failed to deserialize data with json: {e}")

    @staticmethod
    def serialize_list(data: List[Any]) -> str:
        return json.dumps(data)

    @staticmethod
    def deserialize_list(data: str, t: Type) -> List[Any]:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)


class CloudPickleSerializer:
    content_type = "application/octet-stream"
    encoding_type = "cloudpickle"

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
