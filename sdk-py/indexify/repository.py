import aiohttp

from .data_containers import *
from .utils import _get_payload, wait_until


class ARepository:

    def __init__(self, url: str, name: str = "default"):
        self._url = url
        self._name = name

    async def add(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks, "repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/repository/add_texts", json=req) as resp:
                return await _get_payload(resp)


class Repository(ARepository):

    def __init__(self, url, name):
        ARepository.__init__(self, url, name)

    def add(self, *chunks: TextChunk) -> None:
        return wait_until(ARepository.add(self, *chunks))
