from enum import Enum
from typing import Optional, List


class TextSplitter(str, Enum):
    NEWLINE = "new_line"
    REGEX = "regex"
    NOOP = "noop"

    def __str__(self) -> str:
        return self.value.lower()


class TextChunk:
    def __init__(self, text: str, metadata: Optional[dict] = {}):
        self.text = text
        self.metadata = metadata

    def to_dict(self):
        return {"text": self.text, "metadata": self.metadata}


class Message:
    def __init__(self, role: str, text: str, metadata: dict = {}):
        self.role = role
        self.text = text
        self.metadata = metadata

    def to_dict(self):
        return {"role": self.role, "text": self.text, "metadata": self.metadata}


class SearchChunk:
    index: str
    query: str
    k: int

    def to_dict(self):
        return {"index": self.index, "query": self.query, "k": self.k}


class SearchResult:
    results: List[TextChunk]
