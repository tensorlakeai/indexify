import asyncio
import os
from typing import Any, Dict, Optional

import grpc.aio
import yaml

from .metrics.channel_manager import (
    metric_grpc_server_channel_creation_latency,
    metric_grpc_server_channel_creation_retries,
    metric_grpc_server_channel_creations,
)

_RETRY_INTERVAL_SEC = 5
_CONNECT_TIMEOUT_SEC = 5


class ChannelManager:
    def __init__(self, server_address: str, config_path: Optional[str], logger: Any):
        self._logger: Any = logger.bind(module=__name__, server_address=server_address)
        self._keep_alive_period_sec: int = _keep_alive_period_sec_from_env(logger)
        self._server_address: str = server_address
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
                self._channel = await self._create_ready_channel()
            elif not await self._locked_channel_is_healthy():
                self._logger.info("grpc channel to server is unhealthy")
                await self._destroy_locked_channel()
                self._channel = await self._create_ready_channel()

            return self._channel

    def create_channel(self) -> grpc.aio.Channel:
        """Creates a new channel to the gRPC server.

        The channel is not be ready to use. Raises an exception on failure.
        """
        channel_options: list[tuple[str, int]] = _channel_options(
            self._keep_alive_period_sec
        )
        if self._channel_credentials is None:
            return grpc.aio.insecure_channel(
                target=self._server_address, options=channel_options
            )
        else:
            return grpc.aio.secure_channel(
                target=self._server_address,
                credentials=self._channel_credentials,
                options=channel_options,
            )

    async def _create_ready_channel(self) -> grpc.aio.Channel:
        """Creates a new channel to the gRPC server."

        Returns a ready to use channel. Blocks until the channel
        is ready, never raises any exceptions.
        """
        self._logger.info("creating new grpc server channel")

        with metric_grpc_server_channel_creation_latency.time():
            metric_grpc_server_channel_creations.inc()
            while True:
                try:
                    channel = self.create_channel()
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


def _channel_options(keep_alive_period_sec: int) -> list[tuple[str, int]]:
    """Returns the gRPC channel options."""
    # See https://grpc.io/docs/guides/keepalive/.
    #
    # NB: Rust Tonic framework that we're using in Server is not using gRPC core and doesn't support
    # these options. From https://github.com/hyperium/tonic/issues/258 it supports gRPC PINGs when
    # there are in-flight RPCs (and streams) without any extra configuration.
    return [
        ("grpc.keepalive_time_ms", keep_alive_period_sec * 1000),
        (
            "grpc.http2.max_pings_without_data",
            -1,
        ),  # Allow any number of empty PING messages
        (
            "grpc.keepalive_permit_without_calls",
            0,
        ),  # Don't send PINGs when there are no in-flight RPCs (and streams)
    ]


def _keep_alive_period_sec_from_env(logger: Any) -> int:
    """Returns the keep alive period in seconds."""
    # We have to use gRPC keep alive (PING) to prevent proxies/load-balancers from closing underlying HTTP/2
    # (TCP) connections due to periods of idleness in gRPC streams that we use between Executor and Server.
    # If a proxy/load-balancer closes the connection, then we see it as gRPC stream errors which results in
    # a lot of error logs noise.
    #
    # The default period of 50 sec is used for one of the standard proxy/load-balancer timeouts of 1 minute.
    DEFAULT_KEEP_ALIVE_PERIOD_SEC = "50"
    keep_alive_period_sec = int(
        os.getenv(
            "INDEXIFY_EXECUTOR_GRPC_KEEP_ALIVE_PERIOD_SEC",
            DEFAULT_KEEP_ALIVE_PERIOD_SEC,
        )
    )
    if keep_alive_period_sec != int(DEFAULT_KEEP_ALIVE_PERIOD_SEC):
        logger.info(
            f"gRPC keep alive (PING) period is set to {keep_alive_period_sec} sec"
        )
    return keep_alive_period_sec
