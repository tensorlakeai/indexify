import aiohttp

from .data_containers import SearchChunk, TextChunk

class Index:
    def __init__(self, service_url, index):
        self._service_url = service_url
        self._index = index

    def search(self, query: str, top_k: int) -> list[TextChunk]:
        req = {"index": self._index, "query": query, "k": top_k}
        response = aiohttp.post(
            f"{self._service_url}/indexes/{self._index}/search", json=req
        )
        response.raise_for_status()
        return response.json()["results"]