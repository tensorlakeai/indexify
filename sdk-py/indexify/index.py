import requests

from data_containers import *
from utils import _get_payload


class Index:
    DEFAULT_INDEXIFY_URL = "http://localhost:8900"

    def __init__(self, url, index):
        self._url = url
        self._index = index

    def sync(self, name: str, extractor_type):
        pass

    def add(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks}
        resp = requests.post(f"{self._url}/repository/add_texts", json=req)
        if resp.status_code == 200:
            return
        _get_payload(resp)

    def search(self, query: str, top_k: int) -> list[TextChunk]:
        req = SearchChunk(index=self._index, query=query, k=top_k)
        resp = requests.get(f"{self._url}/index/search", json=req.to_dict())
        payload = _get_payload(resp)
        result = []
        for res in payload["results"]:
            result.append(TextChunk(text=res["text"], metadata=res["metadata"]))
        return result
