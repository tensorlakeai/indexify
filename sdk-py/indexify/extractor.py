import httpx 
from dataclasses import dataclass
from typing import Union

from .settings import DEFAULT_SERVICE_URL


@dataclass
class EmbeddingSchema:
    distance: str
    dim: int


@dataclass
class ExtractorSchema:
    outputs: dict[str, Union[EmbeddingSchema, dict]]


@dataclass
class Extractor:
    name: str
    description: str
    input_params: dict
    outputs: ExtractorSchema


class Extractor:
    def __init__(
        self, name: str, description: str, input_params: dict, outputs: ExtractorSchema
    ):
        self.name = name
        self.description = description
        self.input_params = input_params
        self.outputs = outputs 

    @classmethod
    def from_dict(cls, data):
        return Extractor(
            name=data["name"],
            description=data["description"],
            input_params=data["input_params"],
            outputs=data["outputs"],
        )

    def __repr__(self) -> str:
        return f"Extractor(name={self.name}, description={self.description}, input_params={self.input_params}, outputs={self.outputs})"

    def __str__(self) -> str:
        return self.__repr__()
