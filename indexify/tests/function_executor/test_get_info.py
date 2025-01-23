import importlib.metadata
import sys
import unittest

from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    rpc_channel,
)

from indexify.function_executor.proto.function_executor_pb2 import (
    InfoRequest,
    InfoResponse,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)


class TestGetInfo(unittest.TestCase):
    def test_expected_info(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: InfoResponse = stub.get_info(InfoRequest())
                self.assertEqual(response.version, "0.1.0")
                self.assertEqual(response.sdk_language, "python")
                self.assertEqual(
                    response.sdk_version, importlib.metadata.version("tensorlake")
                )
                self.assertEqual(
                    response.sdk_language_version,
                    f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                )


if __name__ == "__main__":
    unittest.main()
