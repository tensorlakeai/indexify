from aiohttp import web

from .handler import Handler


class StartupProbeHandler(Handler):
    def __init__(self):
        self._ready = False

    def set_ready(self):
        self._ready = True

    async def handle(self, request: web.Request) -> web.Response:
        if self._ready:
            return web.json_response({"status": "ok"})
        else:
            return web.json_response({"status": "nok"}, status=503)
