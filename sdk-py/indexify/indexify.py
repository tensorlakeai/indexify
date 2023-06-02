from typing import Optional, List
from enum import Enum
from uuid import UUID
import requests
import json
import dataclasses
from dataclasses import dataclass

DEFAULT_INDEXIFY_URL = "https://localhost:8090"

DEFAULT_EMBEDDING_MODEL = "all-minilm-l6-v2"


class ApiException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class Metric(str, Enum):
    COSINE = "cosine"
    DOT = "dot"
    EUCLIDEAN = "euclidean"

    def __str__(self) -> str:
        return self.name.lower()


class TextSplitter(str, Enum):
    NEWLINE = "new_line"
    REGEX = "regex"
    NOOP = "noop"

    def __str__(self) -> str:
        return self.value.lower()


class CreateIndexArgs:
    name: str
    indexify_url: str
    embedding_model: str
    metric: Metric
    text_splitter: TextSplitter
    hash_on: Optional[List[str]]
    unique_labels: Optional[List[str]]


@dataclass
class TextChunk:
    text: str
    metadata: dict

    def to_json(self):
        return json.dumps({"text": self.text, "metadata": self.metadata})


@dataclass
class SearchChunk:
    index: str
    query: str
    k: int


@dataclass
class SearchResult:
    results: List[TextChunk]


@dataclass
class MemoryStoragePolicy:
    policy_kind: str
    index_name: Optional[str]
    db_url: Optional[str]
    window_size: Optional[int]
    capacity: Optional[int]


@dataclass
class MemoryResult:
    history: List[str]


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
        req = {
            "name": name,
            "embedding_model": embedding_model,
            "metric": metric,
            "text_splitter": splitter,
            "hash_on": unique_labels,
        }
        resp = requests.post(f"{indexify_url}/index/create", json=req)
        if resp.status_code == 200:
            return cls(indexify_url, name)
        Indexify._get_payload(resp)

    @classmethod
    def get_index(cls, name: str, indexify_url: Optional[str]):
        return cls(indexify_url, name)

    def add_text_chunk(self, chunk: str, metadata: dict):
        text_chunk = TextChunk(chunk, metadata)
        req = {"index": self._index, "documents": [dataclasses.asdict(text_chunk)]}
        resp = requests.post(f"{self._url}/index/add", json=req)
        if resp.status_code == 200:
            return
        self._get_payload(resp)

    def search(self, query: str, top_k: int):
        req = SearchChunk(index=self._index, query=query, k=top_k)
        resp = requests.get(f"{self._url}/index/search", json=dataclasses.asdict(req))
        payload = self._get_payload(resp)
        result = SearchResult(results=[])
        for res in payload["results"]:
            result.results.append(TextChunk(text=res["text"], metadata=res["metadata"]))
        return result

    def create_memory_session(
        self,
        session_id: Optional[UUID],
        memory_storage_policy_kind: str,
        index_name: Optional[str],
        db_url: Optional[str],
        window_size: Optional[int],
        capacity: Optional[int],
    ):
        req = MemoryStoragePolicy(
            policy_kind=memory_storage_policy_kind,
            index_name=index_name,
            db_url=db_url,
            window_size=window_size,
            capacity=capacity,
        )
        req = {
            "session_id": session_id,
            "memory_storage_policy": dataclasses.asdict(req),
        }
        resp = requests.post("self._url/memory/create", json=dataclasses.asdict(req))
        payload = self._get_payload(resp)
        return str(payload["results"]["session_id"])

    def add_to_memory(self, session_id: UUID, key: str, value: str):
        req = {"session_id": session_id, "key": key, value: "value"}
        resp = requests.post(f"{self._url}/memory/add", json=req)
        if resp.status_code == 200:
            return
        self._get_payload(resp)

    def retrieve_records(self, session_id: UUID, query: str):
        req = {"session_id": session_id, "query": query}
        resp = requests.post(f"{self._url}/memory/retrieve", json=req)
        payload = self._get_payload(resp)
        return MemoryResult(history=payload["results"]["history"])

    @staticmethod
    def _get_payload(response):
        payload = {"errors": []}
        try:
            payload = json.loads(response.text)
        except:
            raise ApiException(response.text)
        if len(payload["errors"]) > 0:
            raise ApiException(f"Failed to create index: {payload['errors']}")

        return payload
