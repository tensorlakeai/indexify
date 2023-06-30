import requests

from .data_containers import *
from .utils import _get_payload

class Repository:

    def __init__(self, url, name):
        self._url = url
        self._name = name 

    def add(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks}
        resp = requests.post(f"{self._url}/repository/add_texts", json=req)
        if resp.status_code == 200:
            return
        _get_payload(resp)