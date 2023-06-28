import requests

from indexify import *
from utils import _get_payload


class Memory:
    DEFAULT_INDEXIFY_URL = "http://localhost:8900"

    def __init__(self, url, index):
        self._url = url
        self._index = index

    def create(self) -> str:
        resp = requests.post(f"{self._url}/memory/create", json={})
        self.session_id = _get_payload(resp)["session_id"]
        return self.session_id

    def add(self, *messages: Message) -> None:
        parsed_messages = []
        for message in messages:
            parsed_messages.append(message.to_dict())

        req = {"session_id": self.session_id, "messages": parsed_messages}
        resp = requests.post(f"{self._url}/memory/add", json=req)
        if resp.status_code == 200:
            return
        self._get_payload(resp)

    def all(self) -> list[Message]:
        req = {"session_id": self.session_id}
        resp = requests.get(f"{self._url}/memory/get", json=req)
        if resp.status_code == 200:
            payload = self._get_payload(resp)
            messages = []
            for raw_message in payload["messages"]:
                messages.append(Message(raw_message["role"], raw_message["text"], raw_message["metadata"]))
            return messages
        self._get_payload(resp)

