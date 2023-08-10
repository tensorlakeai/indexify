import aiohttp

from .data_containers import SearchChunk, TextChunk
from .utils import _get_payload, wait_until


class AIndex:

    def __init__(self, url: str, index: str = "default/default"):
        self._url = url
        self._index = index

    async def search(self, query: str, top_k: int) -> list[TextChunk]:
        req = SearchChunk(index=self._index, query=query, k=top_k)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._url}/index/search", json=req.to_dict()) as resp:
                payload = await _get_payload(resp)
                result = []
                for res in payload["results"]:
                    result.append(TextChunk(text=res["text"], metadata=res["metadata"]))
                return result


class Index(AIndex):

    def __init__(self, url, index):
        AIndex.__init__(self, url, index)

    def search(self, query: str, top_k: int) -> list[TextChunk]:
        wait_until(AIndex.search(self, query, top_k))

