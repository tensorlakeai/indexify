import aiohttp

from .data_containers import TextChunk
from .settings import DEFAULT_INDEXIFY_URL
from .utils import _get_payload, wait_until


class ARepository:

    def __init__(self, url: str, name: str):
        self._url = url
        self._name = name
        # TODO: add a method for get_or_create_repository()
        # right now you can only use sync repo in server.rs

    async def run_extractors(self) -> dict:
        req = {"repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/repository/run_extractors", json=req) as resp:
                return await _get_payload(resp)

    async def add(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks, "repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/repository/add_texts", json=req) as resp:
                return await _get_payload(resp)

    async def add_documents(self, *documents: dict) -> None:
        req = {"documents": documents, "repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/repository/add_texts", json=req) as resp:
                return await _get_payload(resp)


class Repository(ARepository):

    def __init__(self, url: str = DEFAULT_INDEXIFY_URL, name: str = "default"):
        ARepository.__init__(self, url, name)

    def add(self, *chunks: TextChunk) -> None:
        return wait_until(ARepository.add(self, *chunks))

    def add_documents(self, *documents: dict) -> None:
        return wait_until(ARepository.add_documents(self, *documents))

    def run_extractors(self) -> dict:
        return wait_until(ARepository.run_extractors(self, self._name))
