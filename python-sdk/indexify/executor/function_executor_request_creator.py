from typing import Optional

import httpx
import structlog

from indexify.function_executor.protocol import BinaryData, RunFunctionRequest

from ..common_util import get_httpx_client
from .api_objects import Task

logger = structlog.get_logger(module=__name__)


class FunctionExecutorRequestCreator:
    def __init__(self, base_url: str, config_path: Optional[str] = None):
        self._base_url = base_url
        self._client = get_httpx_client(config_path, make_async=True)

    async def create(self, task: Task) -> RunFunctionRequest:
        """Downloads the function code and input data for the task and creates RunFunctionRequest with them."""
        request = RunFunctionRequest(
            namespace=task.namespace,
            graph_name=task.compute_graph,
            graph_version=task.graph_version,
            graph_invocation_id=task.invocation_id,
            function_name=task.compute_fn,
            task_id=task.id,
            graph=await self._fetch_graph(task),
            graph_invocation_payload=None,
            function_input=None,
            function_init_value=None,
        )
        await self._add_function_inputs(task, request)
        return request

    async def _add_function_inputs(
        self, task: Task, request: RunFunctionRequest
    ) -> None:
        """Downloads the input data for the task and adds it to the request."""
        first_function_in_graph = task.invocation_id == task.input_key.split("|")[-1]
        if first_function_in_graph:
            # The first function in Graph gets its input from graph invocation payload.
            request.graph_invocation_payload = (
                await self._fetch_graph_invocation_payload(task)
            )
        else:
            request.function_input = await self._fetch_function_input(task)

        if task.reducer_output_id is not None:
            request.function_init_value = await self._fetch_function_init_value(task)

    async def _fetch_graph(self, task: Task) -> BinaryData:
        """Downloads the compute graph for the task and returns it."""
        return await self._fetch_url(
            url=f"{self._base_url}/internal/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/code",
            resource_description=f"compute graph: {task.compute_graph}",
        )

    async def _fetch_graph_invocation_payload(self, task: Task) -> BinaryData:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}/invocations/{task.invocation_id}/payload",
            resource_description=f"graph invocation payload: {task.invocation_id}",
        )

    async def _fetch_function_input(self, task: Task) -> BinaryData:
        return await self._fetch_url(
            url=f"{self._base_url}/internal/fn_outputs/{task.input_key}",
            resource_description=f"function input: {task.input_key}",
        )

    async def _fetch_function_init_value(self, task: Task) -> BinaryData:
        return await self._fetch_url(
            url=f"{self._base_url}/namespaces/{task.namespace}/compute_graphs/{task.compute_graph}"
            f"/invocations/{task.invocation_id}/fn/{task.compute_fn}/output/{task.reducer_output_id}",
            resource_description=f"reducer output: {task.reducer_output_id}",
        )

    async def _fetch_url(self, url: str, resource_description: str) -> BinaryData:
        logger.info(f"fetching {resource_description}", url=url)
        response = await self._client.get(url)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(
                f"failed to download {resource_description}",
                error=response.text,
                exc_info=e,
            )
            raise
        return BinaryData(
            data=response.content, content_type=response.headers["content-type"]
        )
