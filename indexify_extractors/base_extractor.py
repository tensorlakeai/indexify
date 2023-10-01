from abc import ABC, abstractmethod
from typing import Any, List, Union, Dict, Literal, Type

from enum import Enum

from pydantic._internal._model_construction import ModelMetaclass

from pydantic import BaseModel, Json, validator, model_serializer


class Embeddings(BaseModel):
    content_id: str
    text: str
    embeddings: List[float]
    metadata: Json

class Attributes(BaseModel):
    content_id: str
    text: str
    attributes: str


class EmbeddingSchema(BaseModel):
    distance: str
    dim: int


class Content(BaseModel):
    id: str
    content_type: str
    data: Any


class ExtractorInfo(BaseModel):
    name: str
    description: str
    input_params: Any
    output_schema: Any

    @model_serializer
    def ser_model(self) -> Dict[str, Any]:
        output_schema_type = "attributes"
        output_schema = self.output_schema.model_json_schema()
        if type(self.output_schema) == EmbeddingSchema:
            output_schema_type = "embedding"
            output_schema = self.output_schema.model_dump()

        return {
            "name": self.name,
            "description": self.description,
            "input_params": self.input_params.model_json_schema() if self.input_params else {},
            "output_schema": {output_schema_type: output_schema},
        }

    @validator("input_params")
    def validate_input_params(cls, val):
        if issubclass(type(val), BaseModel):
            return val

        raise TypeError("Wrong type for 'input_params', must be subclass of BaseModel")

    @validator("output_schema")
    def validate_output_schema(cls, val):
        if issubclass(type(val), EmbeddingSchema):
            return val

        if issubclass(val, BaseModel):
            return val

        raise TypeError(
            f"Wrong type for 'output_schema', must be subclass of BaseModel or EmbeddingSchema {type(val)}"
        )


class Extractor(ABC):
    def _extract(self, content, params: dict[str, Any]):
        content_list = []
        for c in content:
            data = bytearray(c.data)
            if c.content_type == "text":
                data = data.decode("ascii")

            content_list.append(
                Content(
                    id=c.id,
                    content_type=c.content_type,
                    data=data,
                )
            )
        return self.extract(content_list, params)

    @abstractmethod
    def extract(
        self, content: List[Content], params: dict[str, Any]
    ) -> List[Union[Embeddings, Attributes, Type[BaseModel]]]:
        """
        Extracts information from the content.
        """
        pass

    @abstractmethod
    def info(self) -> ExtractorInfo:
        pass

    def _info(self) -> str:
        info = self.info()
        return info.model_dump_json()
