import asyncio
from typing import Any, Optional

import grpc.aio

from .metrics.channel_creator import (
    metric_grpc_server_channel_creation_latency,
    metric_grpc_server_channel_creation_retries,
    metric_grpc_server_channel_creations,
)

_RETRY_INTERVAL_SEC = 5
_CONNECT_TIMEOUT_SEC = 5


class ChannelManager:
    def __init__(self, server_address: str, config_path: Optional[str], logger: Any):
        self._logger: Any = logger.bind(module=__name__)
        self._server_address: str = server_address
        self._config_path: Optional[str] = config_path
        # This lock protects the fields below.
        self._lock = asyncio.Lock()
        self._channel: Optional[grpc.aio.Channel] = None

    async def get_channel(self) -> grpc.aio.Channel:
        """Returns a channel to the gRPC server.

        Returns a ready to use channel. Blocks until the channel is ready,
        never raises any exceptions.
        If previously returned channel is healthy then returns it again.
        Otherwise, returns a new channel but closes the previously returned one.
        """
        async with self._lock:
            if self._channel is None:
                self._channel = await self._create_channel()
            elif not await self._locked_channel_is_healthy():
                self._logger.info("grpc channel to server is unhealthy")
                await self._destroy_locked_channel()
                self._channel = await self._create_channel()

            return self._channel

    async def _create_channel(self) -> grpc.aio.Channel:
        # TODO: Use TLS credentials if provided
        """Creates a new channel to the gRPC server."

        Returns a ready to use channel. Blocks until the channel
        is ready, never raises any exceptions.
        """
        self._logger.info(
            "creating new grpc server channel",
            server_address=self._server_address,
            config_path=self._config_path,
        )

        with metric_grpc_server_channel_creation_latency.time():
            metric_grpc_server_channel_creations.inc()
            while True:
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
                        self._logger.error(
                            "failed closing not established channel", exc_info=e
                        )

                    metric_grpc_server_channel_creation_retries.inc()
                    await asyncio.sleep(_RETRY_INTERVAL_SEC)

    async def _locked_channel_is_healthy(self) -> bool:
        """Checks if the channel is healthy.

        Returns True if the channel is healthy, False otherwise.
        self._lock must be acquired before calling this method.
        Never raises any exceptions.
        """
        try:
            return self._channel.get_state() == grpc.ChannelConnectivity.READY
        except Exception as e:
            # Assume that the channel is healthy because get_state() method is marked as experimental
            # so we can't fully trust it.
            self._logger.error(
                "failed getting channel state, assuming channel is healthy", exc_info=e
            )
            return True

    async def _destroy_locked_channel(self):
        """Closes the existing channel.

        self._lock must be acquired before calling this method.
        Never raises any exceptions.
        """
        try:
            await self._channel.close()
        except Exception as e:
            self._logger.error("failed closing channel", exc_info=e)
        self._channel = None

    async def shutdown(self):
        pass
