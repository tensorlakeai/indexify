import os
import signal
import threading
import time
import unittest

from grpc import RpcError
from tensorlake import Graph
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer
from testing import (
    DEFAULT_FUNCTION_EXECUTOR_PORT,
    FunctionExecutorProcessContextManager,
    rpc_channel,
    run_task,
)

from indexify.function_executor.proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
    InitializeRequest,
    InitializeResponse,
    RunTaskResponse,
    SerializedObject,
)
from indexify.function_executor.proto.function_executor_pb2_grpc import (
    FunctionExecutorStub,
)

# Lower - faster tests but more CPU usage.
HEALTH_CHECK_POLL_PERIOD_SEC = 0.1
HEALTH_CHECK_TIMEOUT_SEC = 5


@tensorlake_function()
def action_function(action: str) -> str:
    if action == "crash_process":
        print("Crashing process...")
        os.kill(os.getpid(), signal.SIGKILL)
    elif action == "deadlock":
        import threading

        lock = threading.Lock()
        lock.acquire()
        lock.acquire()
    elif action == "raise_exception":
        raise Exception("Test exception")
    elif action == "close_connections":
        # 1000 is enough to close server socket.
        os.closerange(0, 1000)
    else:
        return "success"


def initialize(test_case: unittest.TestCase, stub: FunctionExecutorStub):
    initialize_response: InitializeResponse = stub.initialize(
        InitializeRequest(
            namespace="test",
            graph_name="test",
            graph_version="1",
            function_name="action_function",
            graph=SerializedObject(
                bytes=CloudPickleSerializer.serialize(
                    Graph(
                        name="test", description="test", start_node=action_function
                    ).serialize(
                        additional_modules=[],
                    )
                ),
                content_type=CloudPickleSerializer.content_type,
            ),
        )
    )
    test_case.assertTrue(initialize_response.success)


def wait_health_check_failure(test_case: unittest.TestCase, stub: FunctionExecutorStub):
    print("Waiting for health check to fail...")
    HEALTH_CHECK_FAIL_WAIT_SEC = 5
    start_time = time.time()
    while time.time() - start_time < HEALTH_CHECK_FAIL_WAIT_SEC:
        try:
            response: HealthCheckResponse = stub.check_health(
                HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
            )
            test_case.assertTrue(response.healthy)
            time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
        except RpcError:
            return

    test_case.fail(f"Health check didn't fail in {HEALTH_CHECK_FAIL_WAIT_SEC} secs.")


class TestHealthCheck(unittest.TestCase):
    def test_not_initialized_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                response: HealthCheckResponse = stub.check_health(
                    HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                )
                self.assertTrue(response.healthy)

    def test_function_deadlock_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 1
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                def run_task_in_thread():
                    try:
                        run_task(
                            stub,
                            function_name="action_function",
                            input="deadlock",
                            timeout=HEALTH_CHECK_TIMEOUT_SEC,
                        )
                        self.fail("Run task should have timed out.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                print("Waiting for run task thread to fail and unblock...")
                while task_thread.is_alive():
                    response: HealthCheckResponse = stub.check_health(
                        HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                    )
                    self.assertTrue(response.healthy)
                    time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
                task_thread.join()

    def test_function_raises_exception_success(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 2
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                def run_task_in_thread():
                    response: RunTaskResponse = run_task(
                        stub, function_name="action_function", input="raise_exception"
                    )
                    self.assertFalse(response.success)

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                print("Waiting for run task thread to fail and unblock...")
                while task_thread.is_alive():
                    response: HealthCheckResponse = stub.check_health(
                        HealthCheckRequest(), timeout=HEALTH_CHECK_TIMEOUT_SEC
                    )
                    self.assertTrue(response.healthy)
                    time.sleep(HEALTH_CHECK_POLL_PERIOD_SEC)
                task_thread.join()

    def test_process_crash_failure(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 3
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                def run_task_in_thread():
                    try:
                        # Due to "tcp keep-alive" property of the health checks the task should unblock with RpcError.
                        run_task(
                            stub, function_name="action_function", input="crash_process"
                        )
                        self.fail("Run task should have failed.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                wait_health_check_failure(self, stub)
                print("Waiting for run task thread to fail and unblock...")
                task_thread.join()

    def test_process_closes_server_socket_failure(self):
        with FunctionExecutorProcessContextManager(
            DEFAULT_FUNCTION_EXECUTOR_PORT + 4
        ) as process:
            with rpc_channel(process) as channel:
                stub: FunctionExecutorStub = FunctionExecutorStub(channel)
                initialize(self, stub)

                def run_task_in_thread():
                    try:
                        # Due to "tcp keep-alive" property of the health checks the task should unblock with RpcError.
                        run_task(
                            stub,
                            function_name="action_function",
                            input="close_connections",
                        )
                        self.fail("Run task should have failed.")
                    except RpcError:
                        pass

                task_thread = threading.Thread(target=run_task_in_thread)
                task_thread.start()
                wait_health_check_failure(self, stub)
                print("Waiting for run task thread to fail and unblock...")
                task_thread.join()


if __name__ == "__main__":
    unittest.main()
