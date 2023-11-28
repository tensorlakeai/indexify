from enum import Enum


def json_set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


class Metric(str, Enum):
    COSINE = "cosine"
    DOT = "dot"
    EUCLIDEAN = "euclidean"

    def __str__(self) -> str:
        return self.name.lower()
