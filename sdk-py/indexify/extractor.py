import requests
from dataclasses import dataclass
from typing import Union

from .settings import DEFAULT_SERVICE_URL


def list_extractors(base_url: str = DEFAULT_SERVICE_URL) -> list[dict]:
    response = requests.get(f"{base_url}/extractors")
    response.raise_for_status()
    return response.json()["extractors"]


@dataclass
class EmbeddingExtractor:
    dim: int
    distance: str


@dataclass
class AttributeExtractor:
    json_schema: str


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
    schemas: ExtractorSchema


class Extractor:
    def __init__(
        self, name: str, description: str, input_params: dict, schemas: ExtractorSchema
    ):
        self.name = name
        self.description = description
        self.input_params = input_params
        self.schemas = schemas

    @classmethod
    def from_dict(cls, data):
        return Extractor(
            name=data["name"],
            description=data["description"],
            input_params=data["input_params"],
            schemas=data["schemas"],
        )

    def __repr__(self) -> str:
        return f"Extractor(name={self.name}, description={self.description})"

    def __str__(self) -> str:
        return self.__repr__()
