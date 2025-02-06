from aiohttp import web

from .handlers.handler import Handler


class APIServer:
    def __init__(
        self,
        api_host: str,
        api_port: int,
        startup_probe_handler: Handler,
        health_probe_handler: Handler,
    ):
        self._api_host = api_host
        self._api_port = api_port
        self._app: web.Application = web.Application()
        self._app.add_routes(
            [
                web.post("/probe/startup", startup_probe_handler.handle),
                web.post("/probe/health", health_probe_handler.handle),
            ]
        )
        self._app_runner: web.AppRunner = web.AppRunner(self._app)
        # TODO: Configure access logs in struct log.

    async def run(self):
        await self._app_runner.setup()
        site = web.TCPSite(
            runner=self._app_runner,
            host=self._api_host,
            port=self._api_port,
            # Allow to listen when there's a closed socket TIME_WAIT state
            reuse_address=True,
            # Don't allow others to actively listen on this socket to avoid undefined behaviors)
            reuse_port=False,
        )
        await site.start()

    async def shutdown(self):
        await self._app_runner.cleanup()
