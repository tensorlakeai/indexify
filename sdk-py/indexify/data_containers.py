from enum import Enum
from typing import List
from dataclasses import dataclass, field


class TextSplitter(str, Enum):
    NEWLINE = "new_line"
    REGEX = "regex"
    NOOP = "noop"

    def __str__(self) -> str:
        return self.value.lower()


@dataclass
class TextChunk:
    text: str
    metadata: dict[str, any] = field(default_factory=dict)

    def to_dict(self):
        return {"text": self.text, "metadata": self.metadata}


@dataclass
class Message:
    role: str
    text: str
    metadata: dict[str, any] = field(default_factory=dict)

    def to_dict(self):
        return {"role": self.role, "text": self.text, "metadata": self.metadata}


@dataclass
class SearchChunk:
    index: str
    query: str
    k: int

    def to_dict(self):
        return {"index": self.index, "query": self.query, "k": self.k}


@dataclass
class SearchResult:
    results: List[TextChunk]
