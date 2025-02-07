from aiohttp import web

from .handler import Handler


class MonitoringServer:
    def __init__(
        self,
        host: str,
        port: int,
        startup_probe_handler: Handler,
        health_probe_handler: Handler,
        metrics_handler: Handler,
    ):
        self._host = host
        self._port = port
        self._app: web.Application = web.Application()
        self._app.add_routes(
            [
                web.get("/monitoring/startup", startup_probe_handler.handle),
                web.get("/monitoring/health", health_probe_handler.handle),
                web.get("/monitoring/metrics", metrics_handler.handle),
            ]
        )
        self._app_runner: web.AppRunner = web.AppRunner(self._app)

    async def run(self):
        await self._app_runner.setup()
        site = web.TCPSite(
            runner=self._app_runner,
            host=self._host,
            port=self._port,
            # Allow to listen when there's a closed socket in TIME_WAIT state
            reuse_address=True,
            # Don't allow other TCP sockets to actively listen on this address
            reuse_port=False,
        )
        await site.start()

    async def shutdown(self):
        await self._app_runner.cleanup()
