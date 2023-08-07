from abc import ABC, abstractmethod
from typing import Any, List

from enum import Enum

from dataclasses import dataclass
from collections import namedtuple

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
    def extract(self, content: List[Content], params: dict[str, Any]) -> List[Any]:
        """
        Extracts information from the content.
        """
        pass

    @abstractmethod
    def info(self) -> ExtractorInfo:
        pass