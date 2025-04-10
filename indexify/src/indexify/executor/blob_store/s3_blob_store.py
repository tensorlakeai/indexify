import asyncio
from typing import Any, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError as BotoClientError

_MAX_RETRIES = 3


class S3BLOBStore:
    def __init__(self):
        self._s3_client: Optional[Any] = None

    def _lazy_create_client(self):
        """Creates S3 client if it doesn't exist.

        We create the client lazily only if S3 is used.
        This is because S3 BLOB store is always created by Executor
        and the creation will fail if user didn't configure S3 credentials and etc.
        """
        if self._s3_client is not None:
            return

        # The credentials and etc are fetched by boto3 library automatically following
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials
        # This provides a lot of flexibility for the user and follows a well-known and documented logic.
        self._s3_client = boto3.client(
            "s3",
            config=BotoConfig(
                # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html#standard-retry-mode
                retries={
                    "max_attempts": _MAX_RETRIES,
                    "mode": "standard",
                }
            ),
        )

    async def get(self, uri: str, logger: Any) -> bytes:
        """Returns binary value stored in S3 object at the supplied URI.

        The URI must be S3 URI (starts with "s3://").
        Raises Exception on error. Raises KeyError if the object doesn't exist.
        """
        try:
            self._lazy_create_client()
            bucket_name, key = _bucket_name_and_object_key_from_uri(uri)
            response = await asyncio.to_thread(
                self._s3_client.get_object, Bucket=bucket_name, Key=key
            )
            return response["Body"].read()
        except BotoClientError as e:
            logger.error("failed to get S3 object", uri=uri, exc_info=e)

            if e.response["Error"]["Code"] == "NoSuchKey":
                raise KeyError(f"Object {key} does not exist in bucket {bucket_name}")
            raise
        except Exception as e:
            logger.error("failed to get S3 object", uri=uri, exc_info=e)
            raise

    async def put(self, uri: str, value: bytes, logger: Any) -> None:
        """Stores the supplied binary value in a S3 object at the supplied URI.

        The URI must be S3 URI (starts with "s3://").
        Overwrites existing object. Raises Exception on error.
        """
        try:
            self._lazy_create_client()
            bucket_name, key = _bucket_name_and_object_key_from_uri(uri)
            await asyncio.to_thread(
                self._s3_client.put_object, Bucket=bucket_name, Key=key, Body=value
            )
        except Exception as e:
            logger.error("failed to set S3 object", uri=uri, exc_info=e)
            raise


def _bucket_name_and_object_key_from_uri(uri: str) -> tuple[str, str]:
    # Example S3 object URI:
    # s3://test-indexify-server-blob-store-eugene-20250411/225b83f4-2aed-40a7-adee-b7a681f817f2
    if not uri.startswith("s3://"):
        raise ValueError(f"S3 URI '{uri}' is missing 's3://' prefix")

    parts = uri[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Failed parsing bucket name from S3 URI '{uri}'")
    return parts[0], parts[1]  # bucket_name, key
