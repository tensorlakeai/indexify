import json
from .data_containers import *


class ApiException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class Metric(str, Enum):
    COSINE = "cosine"
    DOT = "dot"
    EUCLIDEAN = "euclidean"

    def __str__(self) -> str:
        return self.name.lower()


def _get_payload(response):
    try:
       return json.loads(response.text)
    except:
        raise ApiException(response.text)
