import httpx
from .repository import Repository
from .settings import DEFAULT_SERVICE_URL
from .extractor import Extractor

from typing import List


class IndexifyClient:
    # TODO: update client to handle custom CA certificates for local development
    # or internal deployments with self-signed certificates
    def __init__(self, service_url: str = DEFAULT_SERVICE_URL):
        self._service_url = service_url

    def repositories(self) -> list[Repository]:
        response = httpx.get(f"{self._service_url}/repositories")
        response.raise_for_status()
        repositories_dict = response.json()["repositories"]
        repositories = []
        for rd in repositories_dict:
            repositories.append(Repository(rd["name"], self._service_url))
        return repositories

    def create_repository(
        self, name: str, extractor_bindings: list = [], metadata: dict = {}
    ) -> Repository:
        req = {
            "name": name,
            "extractor_bindings": extractor_bindings,
            "metadata": metadata,
        }
        response = httpx.post(f"{self._service_url}/repositories", json=req)
        response.raise_for_status()
        return Repository(name, self._service_url)

    def get_repository(self, name: str) -> Repository:
        return Repository(name, self._service_url)

    def extractors(self) -> List[Extractor]:
        response = httpx.get(f"{self._service_url}/extractors")
        response.raise_for_status()
        extractors_dict = response.json()["extractors"]
        extractors = []
        for ed in extractors_dict:
            extractors.append(Extractor.from_dict(ed))
        return extractors
