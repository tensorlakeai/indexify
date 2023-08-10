from typing import Any, Coroutine
import aiohttp
import requests

from .index import Index
from .data_containers import TextChunk
from .settings import DEFAULT_INDEXIFY_URL
from .utils import _get_payload, wait_until


def create_repository(name: str, extractors: list = [], metadata: dict = {},
                      base_url: str = DEFAULT_INDEXIFY_URL) -> dict:
    req = {"name": name, "extractors": extractors, "metadata": metadata}
    response = requests.post(f"{base_url}/repository/create", json=req)
    response.raise_for_status()
    return response.json()


def list_repositories(base_url: str = DEFAULT_INDEXIFY_URL) -> list[dict]:
    response = requests.get(f"{base_url}/repository/list")
    response.raise_for_status()
    return response.json()['repositories']


# TODO: consider tying this back to IndexifyExtractor
class RepoExtractor:

    def __init__(self, name: str, index_name: str, filter: dict, input_params: dict):
        self.extractor_name = name
        self.index_name = index_name
        self.filter = filter
        self.input_params = input_params

    def __repr__(self) -> str:
        return f"RepoExtractor(extractor_name={self.extractor_name}, index_name={self.index_name})"

    def __str__(self) -> str:
        return self.__repr__()


class ARepository:

    def __init__(self, base_url: str, name: str):
        self._base_url = base_url
        self._name = name
        self._url = f"{self._base_url}/repository"
        # TODO: self._url = f"{self._base_url}/repository/{self._name}"

    async def run_extractors(self) -> dict:
        req = {"repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/run_extractors", json=req) as resp:
                return await _get_payload(resp)

    async def add(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks, "repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/add_texts", json=req) as resp:
                return await _get_payload(resp)

    async def add_documents(self, *documents: dict) -> None:
        if isinstance(documents[0], dict):
            documents = [documents[0]]  # single document passed
        else:
            documents = documents[0]  # list of documents passed
        req = {"documents": documents, "repository": self._name}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/add_texts", json=req) as resp:
                return await _get_payload(resp)


class Repository(ARepository):

    def __init__(self, base_url: str = DEFAULT_INDEXIFY_URL, name: str = "default"):
        super().__init__(base_url, name)
        if not self._name_exists():
            print(f"creating repo {self._name}")
            create_repository(name=self._name, base_url=self._base_url)

    def add(self, *chunks: TextChunk) -> None:
        return wait_until(ARepository.add(self, *chunks))

    def add_documents(self, *documents: dict) -> None:
        return wait_until(ARepository.add_documents(self, *documents))

    def add_extractor(self, name, index_name, filter) -> dict:
        req = {"repository": self._name, "name": name, "index_name": index_name, "filter": filter}
        response = requests.post(f"{self._url}/add_extractor", json=req)
        response.raise_for_status()
        return response.json()

    @property
    def extractors(self) -> list[RepoExtractor]:
        return [RepoExtractor(**e) for e in self._get_repository_info()['extractors']]

    @property
    def indexes(self) -> list[Index]:
        # TODO: implement this - can take from extractors but not correct
        pass

    def query_attribute(self, index_name: str) -> dict:
        # FIXME: query type should depend on index type
        # TODO: this should be async
        req = {"repository": self._name, "index": index_name}
        response = requests.get(f"{self._url}/attribute_lookup", json=req)
        response.raise_for_status()
        return response.json()['attributes']

    def remove_extractor(self, name) -> dict:
        # TODO: implement this
        pass

    def run_extractors(self) -> dict:
        return wait_until(ARepository.run_extractors(self, self._name))

    def search_index(self, index_name: str, query: str, top_k: int) -> list[TextChunk]:
        # TODO: this should move to index
        # TODO: this should be async
        req = {"repository": self._name, "index": index_name, "query": query, "k": top_k}
        response = requests.get(f"{self._url}/search", json=req)
        response.raise_for_status()
        return response.json()['results']

    def _get_repository_info(self) -> dict:
        req = {"name": self._name}
        response = requests.get(f"{self._url}/get", json=req)
        response.raise_for_status()
        return response.json()['repository']

    def _name_exists(self) -> bool:
        return self._name in [r['name'] for r in list_repositories(self._base_url)]

    def __repr__(self) -> str:
        return f"Repository(name={self._name})"

    def __str__(self) -> str:
        return self.__repr__()
