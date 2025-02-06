from aiohttp import web


class Handler:
    """Abstract base class for all request handlers."""

    async def handle(self, request: web.Request) -> web.Response:
        raise NotImplementedError("Subclasses must implement this method.")
