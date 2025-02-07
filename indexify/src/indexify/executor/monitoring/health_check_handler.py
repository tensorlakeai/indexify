from aiohttp import web

from .handler import Handler
from .health_checker.health_checker import HealthChecker, HealthCheckResult


class HealthCheckHandler(Handler):
    def __init__(self, health_checker: HealthChecker):
        self._health_checker = health_checker

    async def handle(self, request: web.Request) -> web.Response:
        result: HealthCheckResult = await self._health_checker.check()
        return web.json_response(
            {
                "status": "ok" if result.is_success else "nok",
                "message": result.status_message,
                "checker": result.checker_name,
            },
            status=200 if result.is_success else 503,
        )
