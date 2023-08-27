from abc import ABC, abstractmethod
from typing import Any, List, Union

from enum import Enum

from dataclasses import dataclass
from collections import namedtuple

@dataclass
class ExtractedEmbedding:
    content_id: str
    text: str
    embeddings: List[float]

@dataclass
class ExtractedAttributes:
    content_id: str
    json: str

class Datatype(Enum):
    EMBEDDING = 1
    ATTRIBUTES = 2

Content = namedtuple('Content', ['id', 'data'])

@dataclass
class ExtractorInfo:
    name: str
    description: str
    input_params: Any
    output_datatype: Datatype
    output_schema: Any

class Extractor(ABC):

    @abstractmethod
    def extract(self, content: List[Content], params: dict[str, Any]) -> List[Union[ExtractedEmbedding, ExtractedAttributes]]:
        """
        Extracts information from the content.
        """
        pass

    @abstractmethod
    def info(self) -> ExtractorInfo:
        pass