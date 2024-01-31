import httpx
import json

from dataclasses import dataclass
from collections import namedtuple

from .data_containers import TextChunk
from .settings import DEFAULT_SERVICE_URL
from typing import List
from .utils import json_set_default
from indexify.exceptions import ApiException
from .index import Index
from .extractor_binding import ExtractorBinding

Document = namedtuple("Document", ["text", "metadata"])


class Repository:
    def __init__(
        self,
        name: str,
        service_url: str,
        extractor_bindings: List[ExtractorBinding] = None,
        labels: dict = None,
    ) -> None:
        self.name = name
        self._service_url = service_url
        self.extractor_bindings = extractor_bindings
        self.labels = labels

    async def run_extractors(self) -> dict:
        response = httpx.post(f"{self._service_url}/run_extractors")
        response.raise_for_status()

    def add_documents(self, documents: List[Document]) -> None:
        if isinstance(documents, Document):
            documents = [documents]
        req = {"documents": documents}
        response = httpx.post(
            f"{self._service_url}/repositories/{self.name}/add_texts",
            json=req,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

    def bind_extractor(
        self,
        extractor: str,
        name: str,
        input_params: dict = {},
        filters: dict = {},
    ) -> dict:
        """Bind an extractor to this repository

        Args:
            - extractor (str): Name of the extractor
            - name (str): Name for this instance
            - input_params (dict): Dictionary containing extractor input params
            - filter (Filter): Optional filter for this extractor

        Returns:
            dict: response payload

        Examples:
            >>> repo.bind_extractor("EfficientNet", "efficientnet")

            >>> repo.bind_extractor("MiniLML6", "minilm")

        """
        req = {
            "extractor": extractor,
            "name": name,
            "input_params": input_params,
            "filters": filters,
        }

        request_body = json.dumps(req, default=json_set_default)
        response = httpx.post(
            f"{self._service_url}/repositories/{self.name}/extractor_bindings",
            data=request_body,
            headers={"Content-Type": "application/json"},
        )

        # update self.extractor_bindings
        self.get_extractor_bindings()

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ApiException(exc.response.text)
        return

    def indexes(self) -> List[Index]:
        response = httpx.get(f"{self._service_url}/repositories/{self.name}/indexes")
        response.raise_for_status()
        return response.json()["indexes"]

    def upload_file(self, path: str):
        with open(path, "rb") as f:
            response = httpx.post(
                f"{self._service_url}/repositories/{self.name}/upload_file",
                files={"file": f},
            )
            response.raise_for_status()

    @classmethod
    def get(cls, name: str, service_url: str = DEFAULT_SERVICE_URL) -> "Repository":
        response = httpx.get(f"{service_url}/repositories/{name}")
        response.raise_for_status()
        repository_json = response.json()["repository"]
        return Repository._from_json(repository_json)

    @classmethod
    def _from_json(cls, service_url: str, repository_json: dict):
        extractor_bindings = []
        for eb in repository_json["repository"]["extractor_bindings"]:
            extractor_bindings.append(ExtractorBinding.from_dict(eb))
        metadata = repository_json["repository"]["metadata"]
        return Repository(
            name=repository_json["repository"]["name"],
            service_url=service_url,
            extractor_bindings=extractor_bindings,
            metadata=metadata,
        )

    def get_extractor_bindings(self):
        response = httpx.get(f"{self._service_url}/repositories/{self.name}")
        response.raise_for_status()

        self.extractor_bindings = []
        for eb in response.json()["repository"]["extractor_bindings"]:
            self.extractor_bindings.append(ExtractorBinding.from_dict(eb))
        return self.extractor_bindings

    def query_attribute(self, index_name: str, content_id: str = None) -> dict:
        params = {"index": index_name}
        if content_id:
            params.update({"content_id": content_id})
        response = httpx.get(
            f"{self._service_url}/repositories/{self.name}/attributes", params=params
        )
        response.raise_for_status()
        return response.json()["attributes"]

    def search_index(self, name: str, query: str, top_k: int) -> list[TextChunk]:
        req = {"index": name, "query": query, "k": top_k}
        response = httpx.post(
            f"{self._service_url}/repositories/{self.name}/search",
            json=req,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return response.json()["results"]

    def __repr__(self) -> str:
        return f"Repository(name={self.name}, extractor_bindings={self.extractor_bindings}, labels={self.labels})"

    def __str__(self) -> str:
        return self.__repr__()
