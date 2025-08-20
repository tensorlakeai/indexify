from aiohttp import web

from indexify.proto.executor_api_pb2 import (
    ReportExecutorStateRequest,
)

from ..state_reporter import ExecutorStateReporter
from .handler import Handler


class ReportedStateHandler(Handler):
    def __init__(self, state_reporter: ExecutorStateReporter):
        self._state_reporter = state_reporter

    async def handle(self, request: web.Request) -> web.Response:
        request: ReportExecutorStateRequest | None = (
            self._state_reporter.last_state_report_request()
        )
        if request is None:
            return web.Response(status=200, text="No state reported so far")
        else:
            return web.Response(text=str(request))
