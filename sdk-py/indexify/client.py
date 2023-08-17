from .extractor import Extractor, list_extractors
from .repository import Repository, create_repository, list_repositories
from .settings import DEFAULT_SERVICE_URL


class IndexifyClient:

    def __init__(self, service_url: str = DEFAULT_SERVICE_URL):
        self._service_url = service_url

    def create_repository(self, name: str, extractors: list = [], metadata: dict = {}) -> dict:
        return create_repository(name, extractors, metadata, self._service_url)

    @property
    def extractors(self) -> list[Extractor]:
        return [Extractor(**extractor) for extractor in list_extractors(self._service_url)]

    def get_or_create_repository(self, name: str) -> Repository:
        return Repository(name=name, service_url=self._service_url)

    def list_extractors(self) -> list[dict]:
        return list_extractors(base_url=self._service_url)

    def list_repositories(self) -> list[dict]:
        return list_repositories(service_url=self._service_url)

    @property
    def repositories(self) -> list[Repository]:
        # TODO: implement this
        pass
