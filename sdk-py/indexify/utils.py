import asyncio
from enum import Enum
import json


class ApiException(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class Metric(str, Enum):
    COSINE = "cosine"
    DOT = "dot"
    EUCLIDEAN = "euclidean"

    def __str__(self) -> str:
        return self.name.lower()


async def _get_payload(response):
    response.raise_for_status()
    resp = await response.text()
    return json.loads(resp)


def wait_until(functions):
    single_result = False
    if not isinstance(functions, list):
        single_result = True
        functions = [functions]
    holder = []

    async def run_and_capture_result():
        holder.append(await asyncio.gather(*functions))

    asyncio.run(run_and_capture_result())
    if single_result:
        return holder[0][0]  # single result
    else:
        return holder[0]  # list of results
