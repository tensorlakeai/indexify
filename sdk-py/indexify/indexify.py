from typing import Optional, List
from enum import Enum
import requests
import json

DEFAULT_INDEXIFY_URL = "https://localhost:8090"

DEFAULT_EMBEDDING_MODEL = "all-minilm-l6-v2"

class ApiException(Exception):

    def __init__(self, message: str) -> None:
        super().__init__(message)
    
class Metric(str, Enum):
    COSINE = 'cosine'
    DOT = 'dot'
    EUCLIDEAN = 'euclidean'

    def __str__(self) -> str:
        return self.name.lower()


class TextSplitter(str, Enum):
    NEWLINE = "new_line"
    REGEX = "regex"
    NOOP = "noop"

    def __str__(self) -> str:
        return self.value.lower()


class Indexify:
    def __init__(self, url, index) -> None:
        self._url = url
        self._index = index

    @classmethod
    def create_index(
        cls,
        name: str,
        indexify_url: Optional[str] = DEFAULT_INDEXIFY_URL,
        embedding_model: Optional[str] = DEFAULT_EMBEDDING_MODEL,
        metric: Metric = Metric.COSINE,
        splitter: Optional[str] = TextSplitter.NEWLINE,
        unique_labels=Optional[List[str]],
    ):
        req = {"name": name, "embedding_model": embedding_model, "metric": metric, "text_splitter": splitter, "hash_on": unique_labels}
        resp = requests.post(f"{indexify_url}/index/create", json=req)
        if resp.status_code == 200:
            return cls(indexify_url, name)
        payload = json.loads(resp.text)        
        raise ApiException(f"Failed to create index: {payload['errors']}")

    @classmethod
    def get_index(cls, name: str, indexify_url: Optional[str]):
        return cls(indexify_url, name)

    def add_document(self, document: str, attributes: dict):
        pass

    def search(self, query: str, top_k: int):
        pass
