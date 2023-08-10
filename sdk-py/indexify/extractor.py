import requests


from .index import Index
from .data_containers import TextChunk
from .settings import DEFAULT_INDEXIFY_URL
from .utils import _get_payload, wait_until


def list_extractors(base_url: str = DEFAULT_INDEXIFY_URL) -> list[dict]:
    response = requests.get(f"{base_url}/extractors")
    response.raise_for_status()
    return response.json()['extractors']


# TODO: consider naming this IndexifyExtractor
class Extractor:

    def __init__(self, name: str, description: str, extractor_type : dict):
        self.name = name
        self.description = description
        self.extractor_type = extractor_type

    def __repr__(self) -> str:
        return f"Extractor(name={self.name}, description={self.description})"

    def __str__(self) -> str:
        return self.__repr__()
