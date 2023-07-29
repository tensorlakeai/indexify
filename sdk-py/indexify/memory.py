import aiohttp

from .data_containers import *
from .utils import _get_payload, wait_until


class AMemory:

    def __init__(self, url, repository="default"):
        self._session_id = None
        self._url = url
        self._repo = repository

    async def create(self) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/memory/create", json={"repository": self._repo}) as resp:
                resp = await _get_payload(resp)
                self._session_id = resp["session_id"]
        return self._session_id

    async def add(self, *messages: Message) -> None:
        parsed_messages = []
        for message in messages:
            parsed_messages.append(message.to_dict())

        req = {"session_id": self._session_id, "repository": self._repo, "messages": parsed_messages}
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self._url}/memory/add", json=req) as resp:
                return await _get_payload(resp)

    async def all(self) -> list[Message]:
        req = {"session_id": self._session_id, "repository": self._repo}
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self._url}/memory/get", json=req) as resp:
                payload = await _get_payload(resp)
                messages = []
                for raw_message in payload["messages"]:
                    messages.append(Message(raw_message["role"], raw_message["text"], raw_message["metadata"]))
                return messages
            

class Memory(AMemory):
    def __init__(self, url, repository="default"):
        AMemory.__init__(self, url, repository)

    def create(self) -> str:
        return wait_until(AMemory.create(self))

    def add(self, *messages: Message) -> None:
        wait_until(AMemory.add(self, *messages))

    def all(self) -> list[Message]:
        return wait_until(AMemory.all(self))
