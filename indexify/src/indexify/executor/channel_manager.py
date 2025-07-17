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

_RETRY_INTERVAL_SEC = 5


class ChannelManager:
    def __init__(
        self,
        server_address: str,
        config_path: Optional[str],
        logger: Any,
    ):
        self._logger: Any = logger.bind(module=__name__, server_address=server_address)
        self._server_address: str = server_address
        self._channel_credentials: Optional[grpc.ChannelCredentials] = None
        # Shared channel used by different Executor components to communicate with Server.
        self._shared_channel_lock = asyncio.Lock()
        self._shared_channel: Optional[grpc.aio.Channel] = None

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
        # Okay to not hold the lock here as we're destroying the server channel forever.
        if self._shared_channel is not None:
            await self._destroy_shared_channel()

    async def fail_shared_channel(self) -> None:
        """Marks the shared channel as unhealthy and creates a new one.

        Doesn't raise any exceptions.
        """
        async with self._shared_channel_lock:
            if self._shared_channel is None:
                self._logger.error(
                    "grpc server channel doesn't exist, can't mark it unhealthy"
                )
                return

            self._logger.info("marking grpc server channel as unhealthy")
            # All the channel users will see it failing cause we destroyed it and call get_channel() again.
            await self._destroy_shared_channel()

    async def get_shared_channel(self) -> grpc.aio.Channel:
        """Returns shared channel to the gRPC server.

        The health of the shared channel is constantly monitored so it's more reliable than using a
        standalone channel created for a particular short term need. Doesn't raise any exceptions.
        """
        # Use the lock to ensure that we only create one channel without race conditions.
        async with self._shared_channel_lock:
            if self._shared_channel is None:
                await self._create_shared_channel()

            return self._shared_channel

    def create_standalone_channel(self) -> grpc.aio.Channel:
        """Creates a new channel to the gRPC server.

        Used for one-off RPCs where we don't need to monitor channel health or retry its creation indefinitely.
        Raises an exception on failure.
        """
        with (
            metric_grpc_server_channel_creation_retries.count_exceptions(),
            metric_grpc_server_channel_creation_latency.time(),
        ):
            metric_grpc_server_channel_creations.inc()
            if self._channel_credentials is None:
                return grpc.aio.insecure_channel(target=self._server_address)
            else:
                return grpc.aio.secure_channel(
                    target=self._server_address,
                    credentials=self._channel_credentials,
                )

    async def _create_shared_channel(self) -> None:
        """Creates new shared channel.

        self._shared_channel_lock must be acquired before calling this method.
        Never raises any exceptions.
        """
        while True:
            try:
                create_channel_start = time.monotonic()
                self._logger.info("creating new grpc channel to server")
                self._shared_channel = self.create_standalone_channel()
                # Ensure the channel tried to connect to not get "channel closed errors" without actually trying to connect.
                self._shared_channel.get_state(try_to_connect=True)
                self._logger.info(
                    "created new grpc channel to server",
                    duration_sec=time.monotonic() - create_channel_start,
                )
                break
            except Exception as e:
                self._logger.error(
                    f"failed creating grpc channel to server, retrying in {_RETRY_INTERVAL_SEC} seconds",
                    exc_info=e,
                )
                await asyncio.sleep(_RETRY_INTERVAL_SEC)

    async def _destroy_shared_channel(self) -> None:
        """Closes the existing shared channel.

        self._shared_channel_lock must be acquired before calling this method.
        Never raises any exceptions.
        """
        try:
            self._logger.info("closing grpc channel to server")
            await self._shared_channel.close()
            self._logger.info("closed grpc channel to server")
        except Exception as e:
            self._logger.error("failed closing grpc channel to server", exc_info=e)
        self._shared_channel = None
