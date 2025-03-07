import asyncio
from typing import Any

import grpc.aio

_RETRY_INTERVAL_SEC = 5
_CONNECT_TIMEOUT_SEC = 5


class ChannelCreator:
    def __init__(self, server_address: str, logger: Any):
        self._logger = logger.bind(module=__name__)
        self._server_address = server_address
        self._is_shutdown = False

    async def create(self) -> grpc.aio.Channel:
        """Creates a channel to the gRPC server.

        Blocks until the channel is ready.
        Never raises any exceptions.
        """
        while not self._is_shutdown:
            try:
                channel = grpc.aio.insecure_channel(self._server_address)
                await asyncio.wait_for(
                    channel.channel_ready(),
                    timeout=_CONNECT_TIMEOUT_SEC,
                )
                return channel
            except Exception:
                self._logger.error(
                    f"failed establishing grpc server channel in {_CONNECT_TIMEOUT_SEC} sec, retrying in {_RETRY_INTERVAL_SEC} sec"
                )
                try:
                    await channel.close()
                except Exception as e:
                    self._logger.error("failed closing channel", exc_info=e)
                await asyncio.sleep(_RETRY_INTERVAL_SEC)

    async def shutdown(self):
        self._is_shutdown = True
