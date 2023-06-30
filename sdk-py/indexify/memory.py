import requests

from .data_containers import *
from .utils import _get_payload


class Memory:

    def __init__(self, url, repository="default"):
        self._url = url
        self._repo = repository

    def create(self) -> str:
        resp = requests.post(f"{self._url}/memory/create", json={"repository": self._repo})
        self.session_id = _get_payload(resp)["session_id"]
        return self.session_id

    def add(self, *messages: Message) -> None:
        parsed_messages = []
        for message in messages:
            parsed_messages.append(message.to_dict())

        req = {"session_id": self.session_id, "repository": self._repo, "messages": parsed_messages}
        resp = requests.post(f"{self._url}/memory/add", json=req)
        if resp.status_code == 200:
            return
        _get_payload(resp)

    def all(self) -> list[Message]:
        req = {"session_id": self.session_id, "repository": self._repo}
        resp = requests.get(f"{self._url}/memory/get", json=req)
        if resp.status_code == 200:
            payload = _get_payload(resp)
            messages = []
            for raw_message in payload["messages"]:
                messages.append(Message(raw_message["role"], raw_message["text"], raw_message["metadata"]))
            return messages
        _get_payload(resp)

