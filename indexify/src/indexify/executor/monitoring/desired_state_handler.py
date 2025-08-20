from aiohttp import web

from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
)

from ..state_reconciler import ExecutorStateReconciler
from .handler import Handler


class DesiredStateHandler(Handler):
    def __init__(self, state_reconciler: ExecutorStateReconciler):
        self._state_reconciler = state_reconciler

    async def handle(self, request: web.Request) -> web.Response:
        desired_state: DesiredExecutorState | None = (
            self._state_reconciler.get_desired_state()
        )
        if desired_state is None:
            return web.Response(
                status=200, text="No desired state received from Server yet"
            )
        else:
            return web.Response(text=str(desired_state))
