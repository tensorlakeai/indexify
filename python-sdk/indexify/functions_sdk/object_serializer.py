from typing import Any, List

import cloudpickle
import jsonpickle


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
