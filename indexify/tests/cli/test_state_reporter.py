import hashlib
import subprocess
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
from socket import gethostname
from typing import Generator, List

import grpc
import testing
from tensorlake import Graph, RemoteGraph, tensorlake_function
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    function_uri,
    test_graph_name,
    wait_executor_startup,
)

from indexify.proto.executor_api_pb2 import (
    AllowedFunction,
    DesiredExecutorState,
    ExecutorFlavor,
    ExecutorState,
    ExecutorStatus,
    FunctionExecutorState,
    FunctionExecutorStatus,
    GetDesiredExecutorStatesRequest,
    GPUModel,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
)
from indexify.proto.executor_api_pb2_grpc import (
    ExecutorAPIStub,
    add_ExecutorAPIServicer_to_server,
)


class ReportExecutorStateRequestRecorder(ExecutorAPIStub):
    def __init__(self, grpc_server_addr: str):
        self._requests: List[ReportExecutorStateRequest] = []
        self._server: grpc.Server = grpc.server(
            thread_pool=ThreadPoolExecutor(max_workers=1),
        )
        add_ExecutorAPIServicer_to_server(self, self._server)
        self._server.add_insecure_port(grpc_server_addr)
        self._server.start()

    def stop(self):
        self._server.stop(0)

    def report_executor_state(
        self, request: ReportExecutorStateRequest, context: grpc.ServicerContext
    ):
        self._requests.append(request)
        return ReportExecutorStateResponse()

    def get_desired_executor_states(
        self, request: GetDesiredExecutorStatesRequest, context: grpc.ServicerContext
    ) -> Generator[DesiredExecutorState, None, None]:
        raise NotImplementedError()

    def get_requests(self):
        return self._requests

    def wait_until_request_received(self, timeout_sec: int = 10):
        print(
            "Waiting up to 10 secs for a state report request to arrive from Executor..."
        )
        start_time = time.time()
        while not self._requests and time.time() - start_time < timeout_sec:
            time.sleep(0.1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


@tensorlake_function()
def executor_pid_func_1(ignored: int) -> int:
    return executor_pid()


@tensorlake_function()
def executor_pid_func_2(prev_pid: int) -> int:
    return executor_pid()


@tensorlake_function()
def executor_pid_func_3(prev_pid: int) -> int:
    return executor_pid()


@tensorlake_function()
def executor_pid_func_4(prev_pid: int) -> int:
    return executor_pid()


@tensorlake_function()
def executor_pid_func_5(prev_pid: int) -> int:
    return executor_pid()


def expected_executor_state_hash(executor_state: ExecutorState) -> str:
    hashed_state = ExecutorState()
    hashed_state.MergeFrom(executor_state)
    hashed_state.ClearField("state_hash")
    return hashlib.sha256(
        hashed_state.SerializeToString(deterministic=True), usedforsecurity=False
    ).hexdigest()


class TestExecutorStateReporter(unittest.TestCase):
    def verify_new_executor_state(self, request: ReportExecutorStateRequest) -> None:
        self.assertEqual(request.executor_state.executor_id, "test-executor-id")
        self.assertEqual(request.executor_state.development_mode, True)
        self.assertEqual(request.executor_state.hostname, gethostname())
        self.assertEqual(
            request.executor_state.flavor,
            ExecutorFlavor.EXECUTOR_FLAVOR_OSS,
        )
        self.assertEqual(request.executor_state.version, version("indexify"))
        self.assertEqual(
            request.executor_state.status,
            ExecutorStatus.EXECUTOR_STATUS_RUNNING,
        )
        self.assertEqual(request.executor_state.free_resources.cpu_count, 0)
        self.assertEqual(request.executor_state.free_resources.memory_bytes, 0)
        self.assertEqual(request.executor_state.free_resources.disk_bytes, 0)
        self.assertEqual(request.executor_state.free_resources.gpu.count, 0)
        self.assertEqual(
            request.executor_state.free_resources.gpu.model,
            GPUModel.GPU_MODEL_UNKNOWN,
        )

        self.assertEqual(len(request.executor_state.allowed_functions), 1)
        allowed_function: AllowedFunction = request.executor_state.allowed_functions[0]
        self.assertEqual(allowed_function.namespace, "test_namespace")
        self.assertEqual(allowed_function.graph_name, "test_graph_name")
        self.assertEqual(allowed_function.function_name, "test_function_name")
        self.assertFalse(allowed_function.HasField("graph_version"))

        self.assertEqual(len(request.executor_state.labels), 6)
        self.assertEqual(request.executor_state.labels["test_label"], "test_value")

        self.assertEqual(len(request.executor_state.function_executor_states), 0)

        # Verify that the hash is deterministic even when computed in a different process and matches the expected hash.
        self.assertEqual(
            request.executor_state.state_hash,
            expected_executor_state_hash(request.executor_state),
        )

    def test_expected_executor_state_from_new_executor(self) -> None:
        with (
            ReportExecutorStateRequestRecorder("localhost:9000") as recorder,
            ExecutorProcessContextManager(
                [
                    "--executor-id",
                    "test-executor-id",
                    "--dev",
                    "--ports",
                    "60000",
                    "60001",
                    "--monitoring-server-port",
                    "7001",
                    "--grpc-server-addr",
                    "localhost:9000",
                    "--function",
                    function_uri(
                        "test_namespace",
                        "test_graph_name",
                        "test_function_name",
                    ),
                    "--label",
                    "test_label=test_value",
                ]
            ) as executor_a,
        ):
            executor_a: subprocess.Popen
            wait_executor_startup(7001)

            recorder.wait_until_request_received()
            self.assertGreaterEqual(
                len(recorder.get_requests()),
                1,
                "Executor didn't report its state to the server in 10 seconds.",
            )

            for request in recorder.get_requests():
                self.verify_new_executor_state(request)

    def test_expected_executor_state_from_new_executor_with_server_reconnect(
        self,
    ) -> None:
        with ExecutorProcessContextManager(
            [
                "--executor-id",
                "test-executor-id",
                "--dev",
                "--ports",
                "60001",
                "60002",
                "--monitoring-server-port",
                "7002",
                "--grpc-server-addr",
                "localhost:9001",
                "--function",
                function_uri(
                    "test_namespace",
                    "test_graph_name",
                    "test_function_name",
                ),
                "--label",
                "test_label=test_value",
            ],
            keep_std_outputs=False,
        ) as executor_a:
            executor_a: subprocess.Popen
            wait_executor_startup(7002)

            with ReportExecutorStateRequestRecorder("localhost:9001") as recorder:

                recorder.wait_until_request_received()
                self.assertGreaterEqual(
                    len(recorder.get_requests()),
                    1,
                    "Executor didn't report its state to the server in 10 seconds.",
                )

                for request in recorder.get_requests():
                    self.verify_new_executor_state(request)

            print("Waiting 10 secs for executor to notice that server is dead...")
            time.sleep(10)

            with ReportExecutorStateRequestRecorder("localhost:9001") as recorder:
                recorder.wait_until_request_received()
                self.assertGreaterEqual(
                    len(recorder.get_requests()),
                    1,
                    "Executor didn't report its state to the server in 10 seconds.",
                )

                for request in recorder.get_requests():
                    self.verify_new_executor_state(request)

    def test_expected_executor_states_from_executor_running_successful_tasks(self):
        with ExecutorProcessContextManager(
            [
                "--executor-id",
                "test-executor-id",
                "--dev",
                "--ports",
                "60100",
                "60110",
                "--monitoring-server-port",
                "7003",
                "--grpc-server-addr",
                "localhost:9002",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            wait_executor_startup(7003)

            # Run graph with 5 funcs many times and record which one landed on executor_a.
            # Then verify that we have expected Function Executors in the reported state.
            graph = Graph(
                name=test_graph_name(self),
                description="test",
                start_node=executor_pid_func_1,
            )
            graph.add_edge(executor_pid_func_1, executor_pid_func_2)
            graph.add_edge(executor_pid_func_2, executor_pid_func_3)
            graph.add_edge(executor_pid_func_3, executor_pid_func_4)
            graph.add_edge(executor_pid_func_4, executor_pid_func_5)
            remote_graph = RemoteGraph.deploy(graph, additional_modules=[testing])

            all_function_names = [
                "executor_pid_func_1",
                "executor_pid_func_2",
                "executor_pid_func_3",
                "executor_pid_func_4",
                "executor_pid_func_5",
            ]
            expected_function_executor_function_names = set()

            for i in range(10):
                invocation_id = remote_graph.run(block_until_done=True, ignored=0)
                for function_name in all_function_names:
                    output = remote_graph.output(invocation_id, function_name)
                    self.assertEqual(len(output), 1)
                    executor_pid: int = output[0]
                    # Check if the function landed on executor_a and expect its function executor in the state request if True.
                    if executor_pid == executor_a.pid:
                        expected_function_executor_function_names.add(function_name)

            with ReportExecutorStateRequestRecorder("localhost:9002") as recorder:
                recorder.wait_until_request_received()
                self.assertGreaterEqual(
                    len(recorder.get_requests()),
                    1,
                    "Executor didn't report its state to the server in 10 seconds.",
                )
                request: ReportExecutorStateRequest = recorder.get_requests()[0]

                self.assertEqual(request.executor_state.executor_id, "test-executor-id")
                self.assertEqual(request.executor_state.development_mode, True)
                self.assertEqual(
                    request.executor_state.hostname,
                    gethostname(),
                )
                self.assertEqual(
                    request.executor_state.flavor,
                    ExecutorFlavor.EXECUTOR_FLAVOR_OSS,
                )
                self.assertEqual(request.executor_state.version, version("indexify"))
                self.assertEqual(
                    request.executor_state.status,
                    ExecutorStatus.EXECUTOR_STATUS_RUNNING,
                )
                self.assertEqual(request.executor_state.free_resources.cpu_count, 0)
                self.assertEqual(request.executor_state.free_resources.memory_bytes, 0)
                self.assertEqual(request.executor_state.free_resources.disk_bytes, 0)
                self.assertEqual(request.executor_state.free_resources.gpu.count, 0)
                self.assertEqual(
                    request.executor_state.free_resources.gpu.model,
                    GPUModel.GPU_MODEL_UNKNOWN,
                )
                self.assertEqual(len(request.executor_state.labels), 5)

                self.assertEqual(len(request.executor_state.allowed_functions), 0)

                self.assertEqual(
                    len(request.executor_state.function_executor_states),
                    len(expected_function_executor_function_names),
                )
                for (
                    function_executor_state
                ) in request.executor_state.function_executor_states:
                    function_executor_state: FunctionExecutorState
                    self.assertEqual(
                        function_executor_state.status,
                        FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE,
                    )
                    self.assertTrue(function_executor_state.description.HasField("id"))
                    self.assertEqual(
                        function_executor_state.description.namespace, "default"
                    )
                    self.assertEqual(
                        function_executor_state.description.graph_name,
                        test_graph_name(self),
                    )
                    self.assertEqual(
                        function_executor_state.description.graph_version, graph.version
                    )
                    self.assertIn(
                        function_executor_state.description.function_name,
                        expected_function_executor_function_names,
                    )
                    expected_function_executor_function_names.remove(
                        function_executor_state.description.function_name
                    )
                    self.assertFalse(
                        function_executor_state.description.HasField("image_uri")
                    )
                    self.assertFalse(
                        function_executor_state.description.HasField("resource_limits")
                    )

                # Verify that the hash is deterministic even when computed in a different process and matches the expected hash.
                self.assertEqual(
                    request.executor_state.state_hash,
                    expected_executor_state_hash(request.executor_state),
                )


if __name__ == "__main__":
    unittest.main()
