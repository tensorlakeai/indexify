import asyncio

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .handler import Handler


class PrometheusMetricsHandler(Handler):
    async def handle(self, request: web.Request) -> web.Response:
        # Run the synchronous metrics generation code in ThreadPool thread
        # to not block the main asyncio loop.
        return await asyncio.to_thread(self._handle_sync)

    def _handle_sync(self) -> web.Response:
        return web.Response(
            body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )
