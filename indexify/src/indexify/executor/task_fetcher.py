import json
from importlib.metadata import version
from typing import AsyncGenerator, List, Optional

import structlog
from httpx_sse import aconnect_sse
from tensorlake.utils.http_client import get_httpx_client

from .api_objects import ExecutorMetadata, FunctionURI, Task
from .runtime_probes import ProbeInfo, RuntimeProbes


class TaskFetcher:
    """Registers with Indexify server and fetches tasks from it."""

    def __init__(
        self,
        executor_id: str,
        executor_version: str,
        function_allowlist: Optional[List[FunctionURI]],
        protocol: str,
        indexify_server_addr: str,
        config_path: Optional[str] = None,
    ):
        self._protocol: str = protocol
        self._indexify_server_addr: str = indexify_server_addr
        self.config_path = config_path
        self._logger = structlog.get_logger(module=__name__)

        probe_info: ProbeInfo = RuntimeProbes().probe()
        self._executor_metadata: ExecutorMetadata = ExecutorMetadata(
            id=executor_id,
            executor_version=executor_version,
            addr="",
            function_allowlist=function_allowlist,
            labels=probe_info.labels,
        )

    async def run(self) -> AsyncGenerator[Task, None]:
        """Fetches tasks that Indexify server assigned to the Executor.

        Raises an exception if error occurred."""
        url = f"{self._protocol}://{self._indexify_server_addr}/internal/executors/{self._executor_metadata.id}/tasks"

        self._logger.info(
            "registering_executor",
            executor_id=self._executor_metadata.id,
            url=url,
            executor_version=self._executor_metadata.executor_version,
        )
        async with get_httpx_client(
            config_path=self.config_path, make_async=True
        ) as client:
            async with aconnect_sse(
                client,
                "POST",
                url,
                json=self._executor_metadata.model_dump(),
                headers={"Content-Type": "application/json"},
            ) as event_source:
                try:
                    event_source.response.raise_for_status()
                except Exception as e:
                    await event_source.response.aread()
                    raise Exception(
                        "failed to register at server. "
                        f"Response code: {event_source.response.status_code}. "
                        f"Response text: '{event_source.response.text}'."
                    ) from e

                self._logger.info(
                    "executor_registered", executor_id=self._executor_metadata.id
                )
                async for sse in event_source.aiter_sse():
                    task_dicts = json.loads(sse.data)
                    for task_dict in task_dicts:
                        yield Task.model_validate(task_dict, strict=False)
