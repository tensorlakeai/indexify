import asyncio
import time
from typing import Any, Dict, Optional

import grpc.aio
import yaml

from .metrics.channel_manager import (
    metric_grpc_server_channel_creation_latency,
    metric_grpc_server_channel_creation_retries,
    metric_grpc_server_channel_creations,
)
from .monitoring.health_checker.health_checker import HealthChecker

_RETRY_INTERVAL_SEC = 5
_CONNECT_TIMEOUT_SEC = 5


class ChannelManager:
    def __init__(
        self,
        server_address: str,
        config_path: Optional[str],
        health_checker: HealthChecker,
        logger: Any,
    ):
        self._logger: Any = logger.bind(module=__name__, server_address=server_address)
        self._server_address: str = server_address
        self._health_checker: HealthChecker = health_checker
        self._channel_credentials: Optional[grpc.ChannelCredentials] = None
        # This lock protects the fields below.
        self._lock = asyncio.Lock()
        self._channel: Optional[grpc.aio.Channel] = None

        self._init_tls(config_path)

    def _init_tls(self, config_path: Optional[str]):
        if config_path is None:
            return

        # The same config file format as in Tensorlake SDK HTTP client, see:
        # https://github.com/tensorlakeai/tensorlake/blob/main/src/tensorlake/utils/http_client.py
        with open(config_path, "r") as config_file:
            config = yaml.safe_load(config_file)

        if not config.get("use_tls", False):
            return

        tls_config: Dict[str, str] = config["tls_config"]
        cert_path: Optional[str] = tls_config.get("cert_path", None)
        key_path: Optional[str] = tls_config.get("key_path", None)
        ca_bundle_path: Optional[str] = tls_config.get("ca_bundle_path", None)

        self._logger = self._logger.bind(
            cert_path=cert_path,
            key_path=key_path,
            ca_bundle_path=ca_bundle_path,
        )
        self._logger.info("TLS is enabled for grpc channels to server")

        private_key: Optional[bytes] = None
        certificate_chain: Optional[bytes] = None
        root_certificates: Optional[bytes] = None

        if cert_path is not None:
            with open(cert_path, "rb") as cert_file:
                certificate_chain = cert_file.read()
        if key_path is not None:
            with open(key_path, "rb") as key_file:
                private_key = key_file.read()
        if ca_bundle_path is not None:
            with open(ca_bundle_path, "rb") as ca_bundle_file:
                root_certificates = ca_bundle_file.read()

        self._channel_credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )

    async def destroy(self):
        if self._channel is not None:
            await self._destroy_locked_channel()

    async def get_channel(self) -> grpc.aio.Channel:
        """Returns a channel to the gRPC server.

        Returns a ready to use channel. Blocks until the channel is ready,
        never raises any exceptions.
        If previously returned channel is healthy then returns it again.
        Otherwise, returns a new channel but closes the previously returned one.
        """
        # Use the lock to ensure that we only create one channel without race conditions.
        async with self._lock:
            if self._channel is None:
                # Only called on Executor startup when we establish the channel for the first time.
                self._channel = await self._create_ready_channel()
            elif not await self._locked_channel_is_healthy():
                self._logger.info("grpc channel to server is unhealthy")
                self._health_checker.server_connection_state_changed(
                    is_healthy=False,
                    status_message="grpc channel to server is unhealthy",
                )
                await self._destroy_locked_channel()
                self._channel = await self._create_ready_channel()
                self._health_checker.server_connection_state_changed(
                    is_healthy=True, status_message="grpc channel to server is healthy"
                )

            return self._channel

    def create_channel(self) -> grpc.aio.Channel:
        """Creates a new channel to the gRPC server.

        The channel is not ready to use. Raises an exception on failure.
        """
        if self._channel_credentials is None:
            return grpc.aio.insecure_channel(target=self._server_address)
        else:
            return grpc.aio.secure_channel(
                target=self._server_address,
                credentials=self._channel_credentials,
            )

    async def _create_ready_channel(self) -> grpc.aio.Channel:
        """Creates a new channel to the gRPC server."

        Returns a ready to use channel. Blocks until the channel
        is ready, never raises any exceptions.
        """
        with metric_grpc_server_channel_creation_latency.time():
            metric_grpc_server_channel_creations.inc()
            while True:
                try:
                    self._logger.info("creating new grpc server channel")
                    create_channel_start = time.monotonic()
                    channel: grpc.Channel = self.create_channel()
                    self._logger.info(
                        "grpc server channel created",
                        duration_sec=time.monotonic() - create_channel_start,
                    )

                    channel_ready_start = time.monotonic()
                    await asyncio.wait_for(
                        channel.channel_ready(),
                        timeout=_CONNECT_TIMEOUT_SEC,
                    )
                    self._logger.info(
                        "grpc server channel is established (ready)",
                        duration_sec=time.monotonic() - channel_ready_start,
                    )

                    return channel
                except BaseException:
                    self._logger.error(
                        f"failed establishing grpc server channel in {_CONNECT_TIMEOUT_SEC} sec, retrying in {_RETRY_INTERVAL_SEC} sec"
                    )
                    try:
                        await channel.close()
                    except BaseException as e:
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
