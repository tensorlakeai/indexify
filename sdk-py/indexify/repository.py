import aiohttp
import requests

from .index import Index
from .data_containers import TextChunk
from .settings import DEFAULT_SERVICE_URL
from .utils import _get_payload, wait_until


def create_repository(name: str, extractors: list = (), metadata: dict = {},
                      service_url: str = DEFAULT_SERVICE_URL) -> dict:
    req = {"name": name, "extractors": extractors, "metadata": metadata}
    response = requests.post(f"{service_url}/repositories", json=req)
    response.raise_for_status()
    return response.json()


def list_repositories(service_url: str = DEFAULT_SERVICE_URL) -> list[dict]:
    response = requests.get(f"{service_url}/repositories")
    response.raise_for_status()
    return response.json()['repositories']


# TODO: consider tying this back to IndexifyExtractor
class ExtractorBinding:

    def __init__(self, extractor_name: str, index_name: str, filters: dict, input_params: dict):
        self.extractor_name = extractor_name
        self.index_name = index_name
        self.filters = filters
        self.input_params = input_params

    def __repr__(self) -> str:
        return f"ExtractorBinding(extractor_name={self.extractor_name}, index_name={self.index_name})"

    def __str__(self) -> str:
        return self.__repr__()


class ARepository:

    def __init__(self, name: str, service_url: str):
        self.name = name
        self._service_url = service_url
        self.url = f"{self._service_url}/repositories/{self.name}"

    async def run_extractors(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.url}/run_extractors") as resp:
                return await _get_payload(resp)

    async def add_documents(self, *documents: dict) -> None:
        if isinstance(documents[0], dict):
            documents = [documents[0]]  # single document passed
        else:
            documents = documents[0]  # list of documents passed
        for doc in documents:
            if "metadata" not in doc:
                doc.update({"metadata": {}})
        req = {"documents": documents}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.url}/add_texts", json=req) as resp:
                return await _get_payload(resp)


class Repository(ARepository):

    def __init__(self, name: str = "default", service_url: str = DEFAULT_SERVICE_URL):
        super().__init__(name, service_url)
        if not self._name_exists():
            print(f"creating repo {self.name}")
            create_repository(name=self.name, service_url=self._service_url)

    def add_documents(self, *documents: dict) -> None:
        return wait_until(ARepository.add_documents(self, *documents))

    def bind_extractor(self, extractor_name: str, index_name: str,
                       include: dict | None = None,
                       exclude: dict | None = None) -> dict:
        """Bind an extractor to this repository

        Args:
            extractor_name (str): Name of extractor
            index_name (str): Name of corresponding index
            include (dict | None, optional): Conditions that must be true
                for an extractor to run on a document in the repository.
                Defaults to None.
            exclude (dict | None, optional): Conditions that must be false
                for an extractor to run on a document in the repository.
                Defaults to None.

        Returns:
            dict: response payload

        Examples:
            >>> repo.bind_extractor("EfficientNet", "png_embeddings",
                                    include={"file_ext": "png"})

            >>> repo.bind_extractor("MiniLML6", "non_english",
                                    exclude={"language": "en"})

        """
        filters = []
        if include is not None:
            filters.extend([{'eq': {k: v}} for k, v in include.items()])
        if exclude is not None:
            filters.extend([{'ne': {k: v}} for k, v in exclude.items()])
        req = {"extractor_name": extractor_name,
               "index_name": index_name,
               "filters": filters}
        response = requests.post(f"{self.url}/extractor_bindings", json=req)
        response.raise_for_status()
        return response.json()

    @property
    def extractor_bindings(self) -> list[ExtractorBinding]:
        return [ExtractorBinding(**e) for e in self._get_repository_info()['extractor_bindings']]

    @property
    def indexes(self) -> list[Index]:
        # TODO: implement this - can take from extractors but not correct
        pass

    # FIXME: query type should depend on index type
    def query_attribute(self, index_name: str, content_id: str = None) -> dict:
        # TODO: this should be async
        params = {"index": index_name}
        if content_id:
            params.update({"content_id": content_id})
        response = requests.get(f"{self.url}/attributes", params=params)
        response.raise_for_status()
        return response.json()['attributes']

    def unbind_extractor(self, name) -> dict:
        # TODO: implement this
        pass

    def run_extractors(self) -> dict:
        return wait_until(ARepository.run_extractors(self, self.name))

    # TODO: this should move to index
    def search_index(self, index_name: str, query: str, top_k: int) -> list[TextChunk]:
        # TODO: this should be async
        req = {"index": index_name, "query": query, "k": top_k}
        response = requests.post(f"{self.url}/search", json=req)
        response.raise_for_status()
        return response.json()['results']

    def _get_repository_info(self) -> dict:
        response = requests.get(f"{self.url}")
        response.raise_for_status()
        return response.json()['repository']

    def _name_exists(self) -> bool:
        return self.name in [r['name'] for r in list_repositories(self._service_url)]

    def __repr__(self) -> str:
        return f"Repository(name={self.name})"

    def __str__(self) -> str:
        return self.__repr__()
