from concurrent.futures import ThreadPoolExecutor

import grpc

from .function_executor_service import FunctionExecutorService
from .proto.function_executor_pb2_grpc import (
    add_FunctionExecutorServicer_to_server,
)

# Temporary limit until we have a better way to control this.
# This limits the number of concurrent tasks that Function Executor can run.
MAX_RPC_CONCURRENCY = 100


class Server:
    def __init__(self, server_address: str, service: FunctionExecutorService):
        self._server_address: str = server_address
        self._service: FunctionExecutorService = service

    def run(self):
        """Runs Function Executor Service at the configured address."""
        server = grpc.server(ThreadPoolExecutor(max_workers=MAX_RPC_CONCURRENCY))
        add_FunctionExecutorServicer_to_server(self._service, server)
        server.add_insecure_port(self._server_address)
        server.start()
        server.wait_for_termination()
