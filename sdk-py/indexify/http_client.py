import json
import requests

from data_containers import *


class Indexify:
    DEFAULT_INDEXIFY_URL = "http://localhost:8900"
    DEFAULT_EMBEDDING_MODEL = "all-minilm-l6-v2"

    def __init__(self, url, index):
        self._url = url
        self._index = index

    def add_text(self, *chunks: TextChunk) -> None:
        parsed_chunks = []
        for chunk in chunks:
            parsed_chunks.append(chunk.to_dict())
        req = {"documents": parsed_chunks}
        resp = requests.post(f"{self._url}/repository/add_texts", json=req)
        if resp.status_code == 200:
            return
        self._get_payload(resp)

    def search(self, query: str, top_k: int) -> list[TextChunk]:
        req = SearchChunk(index=self._index, query=query, k=top_k)
        resp = requests.get(f"{self._url}/index/search", json=req.to_dict())
        payload = self._get_payload(resp)
        result = []
        for res in payload["results"]:
            result.append(TextChunk(text=res["text"], metadata=res["metadata"]))
        return result

    def create_memory(self) -> str:
        resp = requests.post(f"{self._url}/memory/create", json={})
        self.session_id = Indexify._get_payload(resp)["session_id"]
        return self.session_id

    def add_to_memory(self, *messages: Message) -> None:
        parsed_messages = []
        for message in messages:
            parsed_messages.append(message.to_dict())

        req = {"session_id": self.session_id, "messages": parsed_messages}
        resp = requests.post(f"{self._url}/memory/add", json=req)
        if resp.status_code == 200:
            return
        self._get_payload(resp)

    def all_memory(self) -> list[Message]:
        req = {"session_id": self.session_id}
        resp = requests.get(f"{self._url}/memory/get", json=req)
        if resp.status_code == 200:
            payload = self._get_payload(resp)
            messages = []
            for raw_message in payload["messages"]:
                messages.append(Message(raw_message["role"], raw_message["text"], raw_message["metadata"]))
            return messages
        self._get_payload(resp)

    @staticmethod
    def _get_payload(response):
        payload = {"errors": []}
        try:
            payload = json.loads(response.text)
        except:
            raise ApiException(response.text)
        if "errors" in payload.keys() and len(payload["errors"]) > 0:
            raise ApiException(f"Failed to create index: {payload['errors']}")

        return payload


class ApiException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class Metric(str, Enum):
    COSINE = "cosine"
    DOT = "dot"
    EUCLIDEAN = "euclidean"

    def __str__(self) -> str:
        return self.name.lower()
