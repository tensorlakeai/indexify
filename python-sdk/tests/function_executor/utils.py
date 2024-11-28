import os
import subprocess
import unittest

import grpc

from indexify.function_executor.proto.configuration import GRPC_CHANNEL_OPTIONS


class FunctionExecutorServerTestCase(unittest.TestCase):
    FUNCTION_EXECUTOR_SERVER_ADDRESS = "localhost:50000"
    _functionExecutorServerProc: subprocess.Popen = None

    @classmethod
    def setUpClass(cls):
        # We setup one Function Executor server for all the tests running in this class.
        # If an exception is raised during a setUpClass then the tests in the class are
        # not run and the tearDownClass is not run.
        cls._functionExecutorServerProc = subprocess.Popen(
            [
                "indexify-cli",
                "function-executor",
                "--function-executor-server-address",
                cls.FUNCTION_EXECUTOR_SERVER_ADDRESS,
                "--indexify-server-address",
                os.environ.get("INDEXIFY_URL"),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        if cls._functionExecutorServerProc is not None:
            cls._functionExecutorServerProc.kill()
            cls._functionExecutorServerProc.wait()

    def _rpc_channel(self) -> grpc.Channel:
        channel: grpc.Channel = grpc.insecure_channel(
            self.FUNCTION_EXECUTOR_SERVER_ADDRESS,
            options=GRPC_CHANNEL_OPTIONS,
        )
        try:
            SERVER_STARTUP_TIMEOUT_SEC = 5
            # This is not asyncio.Future but grpc.Future. It has a different interface.
            grpc.channel_ready_future(channel).result(
                timeout=SERVER_STARTUP_TIMEOUT_SEC
            )
            return channel
        except Exception as e:
            channel.close()
            self.fail(
                f"Failed to connect to the gRPC server within {SERVER_STARTUP_TIMEOUT_SEC} seconds: {e}"
            )
