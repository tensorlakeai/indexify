import asyncio
import os
import signal
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Generator, List, Optional
from unittest.mock import MagicMock

import grpc
import structlog
from tensorlake import Graph, TensorlakeCompute, tensorlake_function
from tensorlake.function_executor.proto.function_executor_pb2 import (
    FunctionOutput,
    SerializedObject,
)
from tensorlake.functions_sdk.object_serializer import CloudPickleSerializer

from indexify.executor.downloader import Downloader
from indexify.executor.executor_flavor import ExecutorFlavor
from indexify.executor.function_executor.function_executor_states_container import (
    FunctionExecutorStatesContainer,
)
from indexify.executor.function_executor.server.subprocess_function_executor_server_factory import (
    SubprocessFunctionExecutorServerFactory,
)
from indexify.executor.function_executor.task_output import TaskOutput
from indexify.executor.grpc.channel_manager import ChannelManager
from indexify.executor.grpc.state_reconciler import ExecutorStateReconciler
from indexify.executor.grpc.state_reporter import ExecutorStateReporter
from indexify.executor.task_reporter import TaskReporter
from indexify.proto.executor_api_pb2 import (
    DesiredExecutorState,
    ExecutorState,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    GetDesiredExecutorStatesRequest,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
    ReportTaskOutcomeRequest,
    ReportTaskOutcomeResponse,
    Task,
    TaskAllocation,
)
from indexify.proto.executor_api_pb2_grpc import (
    ExecutorAPIStub,
    add_ExecutorAPIServicer_to_server,
)

# Unit tests for ExecutorStateReconciler class.
# If it gets expensive to maintain it then we can remove it once we migrated to grpc.


@tensorlake_function()
def function_a(input: str) -> str:
    if input == "raise_exception":
        raise Exception("Test exception")
    elif input == "sleep_forever":
        time.sleep(1000000)
    else:
        return f"function_a: {input}"


@tensorlake_function()
def function_b(input: str) -> str:
    return f"function_b: {input}"


@tensorlake_function()
def function_c(input: str) -> str:
    return f"function_c: {input}"


def create_test_graph() -> SerializedObject:
    graph = Graph(name="test", description="test", start_node=function_a)
    graph = graph.add_edge(function_a, function_b)
    graph = graph.add_edge(function_b, function_c)
    return SerializedObject(
        bytes=CloudPickleSerializer.serialize(
            graph.serialize(
                additional_modules=[],
            )
        ),
        content_type=CloudPickleSerializer.content_type,
    )


def create_test_input(input: str) -> SerializedObject:
    return SerializedObject(
        bytes=CloudPickleSerializer.serialize(input),
        content_type=CloudPickleSerializer.content_type,
    )


class FunctionThatFailsToInitialize(TensorlakeCompute):
    name = "FunctionThatFailsToInitialize"

    def __init__(self):
        super().__init__()
        raise Exception("Test Exception!")

    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


def create_test_graph_with_function(
    function_class: TensorlakeCompute,
) -> SerializedObject:
    graph = Graph(
        name="test-graph-with-function",
        description="test",
        start_node=function_class,
    )
    return SerializedObject(
        bytes=CloudPickleSerializer.serialize(
            graph.serialize(
                additional_modules=[],
            )
        ),
        content_type=CloudPickleSerializer.content_type,
    )


class FunctionThatSleepsForeverOnInitialization(TensorlakeCompute):
    name = "FunctionThatSleepsForeverOnInitialization"

    def __init__(self):
        super().__init__()
        time.sleep(1000000)

    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


class FunctionThatKillsCurrentProcessIn10SecsAfterInitialization(TensorlakeCompute):
    name = "FunctionThatKillsCurrentProcessIn10SecsAfterInitialization"

    def __init__(self):
        super().__init__()

        def kill_process():
            time.sleep(10)
            os.kill(os.getpid(), signal.SIGKILL)

        threading.Thread(target=kill_process, daemon=True).start()

    def run(self, action: str) -> str:
        raise Exception("This function shouldn't run.")


class ExecutorAPIServerTestScenario(ExecutorAPIStub):
    def __init__(
        self,
        grpc_server_addr: str,
        desired_executor_states_sent_to_executor: Generator[
            DesiredExecutorState, None, None
        ],
        executor_states_reported_by_executor: List[ExecutorState],
    ):
        self._desired_executor_states_sent_to_executor = (
            desired_executor_states_sent_to_executor
        )
        self._executor_states_reported_by_executor = (
            executor_states_reported_by_executor
        )
        self._server: grpc.Server = grpc.server(
            thread_pool=ThreadPoolExecutor(max_workers=100),
        )
        add_ExecutorAPIServicer_to_server(self, self._server)
        self._server.add_insecure_port(grpc_server_addr)
        self._server.start()

    def stop(self):
        self._server.stop(0)

    def report_executor_state(
        self, request: ReportExecutorStateRequest, context: grpc.ServicerContext
    ):
        self._executor_states_reported_by_executor.append(request.executor_state)
        return ReportExecutorStateResponse()

    def get_desired_executor_states(
        self, request: GetDesiredExecutorStatesRequest, context: grpc.ServicerContext
    ) -> Generator[DesiredExecutorState, None, None]:
        yield from self._desired_executor_states_sent_to_executor()

    def report_task_outcome(
        self, request: ReportTaskOutcomeRequest, context: grpc.ServicerContext
    ) -> ReportTaskOutcomeResponse:
        # We mock task reporter in tests, so we don't need to implement this method.
        raise NotImplementedError()


class DesiredStatesGenerator:
    def __init__(self):
        self._shutdown = False
        self._clock = 0
        self._desired_state: Optional[DesiredExecutorState] = None

    def shutdown(self):
        self._shutdown = True

    def set_desired_state(self, state: DesiredExecutorState):
        self._desired_state = state

    def clock(self) -> int:
        return self._clock

    def __call__(self) -> Generator[DesiredExecutorState, None, None]:
        last_reported_desired_state = None
        while not self._shutdown:
            # Only report the desired state if it changed. This is closer to
            # Server behavior and ensures that Executor doesn't get stuck if new
            # desired state is not sent.
            if last_reported_desired_state != self._desired_state:
                last_reported_desired_state = self._desired_state
                self._clock += 1
                state = DesiredExecutorState(clock=self._clock)
                state.MergeFrom(self._desired_state)
                yield state
            time.sleep(0.1)


async def wait_condition(predicate: Callable[[], bool], timeout_sec: float = 10.0):
    deadline_sec = time.monotonic() + timeout_sec
    while time.monotonic() < deadline_sec:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Condition {predicate} not met within the timeout.")


class TestExecutorStateReconciler(unittest.IsolatedAsyncioTestCase):
    API_SERVER_ADDRESS = "localhost:8902"
    fe_server_factory: SubprocessFunctionExecutorServerFactory = None

    @classmethod
    def setUpClass(cls):
        # Reuse the factory across different tests to not care about port overlaps.
        cls.fe_server_factory = SubprocessFunctionExecutorServerFactory(
            development_mode=True,
            server_ports=range(60000, 60500),
        )

    async def asyncSetUp(self) -> None:
        logger = structlog.get_logger(module=__name__)

        self.mock_downloader = MagicMock(spec=Downloader, name="Mock Downloader")
        self.mock_downloader.download_graph.return_value = SerializedObject()  # TODO
        self.mock_downloader.download_input.return_value = SerializedObject()  # TODO

        self.mock_task_reporter = MagicMock(
            spec=TaskReporter, name="Mock Task Reporter"
        )
        self.mock_task_reporter.report.return_value = None

        self.channel_manager = ChannelManager(
            server_address=self.API_SERVER_ADDRESS, config_path=None, logger=logger
        )
        self.function_executor_states = FunctionExecutorStatesContainer(logger=logger)

        self.state_reporter = ExecutorStateReporter(
            executor_id="test-executor-id",
            flavor=ExecutorFlavor.OSS,
            version="test-version",
            labels={"test": "true"},
            development_mode=True,
            function_allowlist=[],
            function_executor_states=self.function_executor_states,
            channel_manager=self.channel_manager,
            logger=logger,
            reporting_interval_sec=0.1,  # Speed up tests using the shorter interval.
        )
        self.state_reporter_task = asyncio.create_task(self.state_reporter.run())

        self.state_reconciler = ExecutorStateReconciler(
            executor_id="test-executor-id",
            function_executor_server_factory=self.fe_server_factory,
            # Dummy URL for invocation state API, we are not using it in this test.
            base_url="http://localhost/this_is_a_dummy_url",
            function_executor_states=self.function_executor_states,
            config_path=None,
            downloader=self.mock_downloader,
            task_reporter=self.mock_task_reporter,
            channel_manager=self.channel_manager,
            state_reporter=self.state_reporter,
            logger=logger,
        )
        self.state_reconciler_task = asyncio.create_task(self.state_reconciler.run())
        self.desired_states_generator = DesiredStatesGenerator()
        # Create by the test case code.
        self.executor_api_server_test_scenario: ExecutorAPIServerTestScenario = None

    async def asyncTearDown(self):
        await self.state_reconciler.shutdown()
        await self.state_reporter.shutdown()
        await self.channel_manager.destroy()
        await self.function_executor_states.shutdown()

        self.state_reporter_task.cancel()
        try:
            await self.state_reporter_task
        except asyncio.CancelledError:
            pass  # expected

        self.state_reconciler_task.cancel()
        try:
            await self.state_reconciler_task
        except asyncio.CancelledError:
            pass

        self.desired_states_generator.shutdown()
        if self.executor_api_server_test_scenario is not None:
            self.executor_api_server_test_scenario.stop()

    def create_executor_api_server_test_scenario(
        self,
        desired_executor_states_sent_to_executor: Generator[
            DesiredExecutorState, None, None
        ],
        executor_states_reported_by_executor: List[ExecutorState],
    ) -> None:
        self.executor_api_server_test_scenario = ExecutorAPIServerTestScenario(
            self.API_SERVER_ADDRESS,
            desired_executor_states_sent_to_executor,
            executor_states_reported_by_executor,
        )

    def deserialize_function_output(self, function_output: FunctionOutput) -> List[Any]:
        outputs: List[Any] = []
        for output in function_output.outputs:
            # Only implement Cloudpickle for simplicity.
            self.assertEqual(output.content_type, CloudPickleSerializer.content_type)
            outputs.append(CloudPickleSerializer.deserialize(output.bytes))
        return outputs

    async def test_no_function_executors_and_task_in_desired_states(self):
        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def five_states_reported_by_executor() -> bool:
            return len(reported_states) >= 5

        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )
        await wait_condition(five_states_reported_by_executor, timeout_sec=30)
        for state in reported_states:
            state: ExecutorState
            self.assertEqual(len(state.function_executor_states), 0)
            if state.HasField("server_clock"):
                self.assertLessEqual(
                    state.server_clock, self.desired_states_generator.clock()
                )

        # At least one of the state should be reconciled already and thus provide server clock.
        latest_state: ExecutorState = reported_states[-1]
        self.assertTrue(latest_state.HasField("server_clock"))

        # Verify expcted mock calls.
        self.mock_downloader.download_graph.assert_not_called()
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_function_executor_from_malformed_graph_and_destroy_it(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="test_function_one",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            SerializedObject()
        )  # malformed graph

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_platform_error_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR
            )

        await wait_condition(function_executor_has_platform_error_status)
        self.assertTrue(function_executor_has_platform_error_status())

        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)
        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )
        fe_state: FunctionExecutorState = reported_states[-1].function_executor_states[
            0
        ]
        self.assertEqual(fe_state.description.id, "fe-1")
        self.assertEqual(fe_state.description.namespace, "test-namespace-1")
        self.assertEqual(fe_state.description.graph_name, "test-graph-1")
        self.assertEqual(fe_state.description.graph_version, "test-version-1")
        self.assertEqual(fe_state.description.function_name, "test_function_one")
        self.assertFalse(fe_state.description.HasField("image_uri"))
        self.assertFalse(fe_state.description.HasField("resource_limits"))
        self.assertFalse(fe_state.description.HasField("customer_code_timeout_ms"))
        self.assertEqual(len(fe_state.description.secret_names), 0)

        # Verify that Executor destroyed the failed to startup FE to release its resources asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Ask reconciler to delete the FE.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_function_executor_with_function_that_fails_to_initialize_and_destroy_it(
        self,
    ):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatFailsToInitialize",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            create_test_graph_with_function(FunctionThatFailsToInitialize)
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_startup_failed_customer_error_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR
            )

        await wait_condition(function_executor_has_startup_failed_customer_error_status)
        self.assertTrue(function_executor_has_startup_failed_customer_error_status())

        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)
        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )
        fe_state: FunctionExecutorState = reported_states[-1].function_executor_states[
            0
        ]
        # This is how Server gets customer error from failed to initialize FE.
        self.assertEqual(fe_state.status_message, "Test Exception!")

        # Verify that Executor destroyed the failed to startup FE to release its resources asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Ask reconciler to delete the FE.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_function_executor_with_customer_initialization_code_timeout_and_destroy_it(
        self,
    ):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatSleepsForeverOnInitialization",
                        customer_code_timeout_ms=5 * 1000,  # 5 seconds
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            create_test_graph_with_function(FunctionThatSleepsForeverOnInitialization)
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_startup_failed_customer_error_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_CUSTOMER_ERROR
            )

        await wait_condition(function_executor_has_startup_failed_customer_error_status)
        self.assertTrue(function_executor_has_startup_failed_customer_error_status())

        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)
        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )
        fe_state: FunctionExecutorState = reported_states[-1].function_executor_states[
            0
        ]
        # This is how Server gets customer error from failed to initialize FE.
        self.assertEqual(
            fe_state.status_message, f"Customer code timeout of 5.000 sec expired"
        )

        # Verify that Executor destroyed the failed to startup FE to release its resources asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Ask reconciler to delete the FE.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    # TODO: We need be able to cancel Function Executor startup at any moment to give more control to Server.
    async def test_remove_function_executor_while_starting_it_up_nothing_happens(
        self,
    ):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatSleepsForeverOnInitialization",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            create_test_graph_with_function(FunctionThatSleepsForeverOnInitialization)
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_is_starting_up() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTING_UP
            )

        await wait_condition(function_executor_is_starting_up)
        self.assertTrue(function_executor_is_starting_up())

        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)
        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )

        # Ask reconciler to delete the FE that is starting up.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        # Wait for reconciler to react in any way.
        await asyncio.sleep(10)
        # To shutdown an FE it first needs to finish starting up and this FE never finishes starting up.
        self.assertTrue(function_executor_is_starting_up())
        self.assertEqual(len(self.function_executor_states._states), 1)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_function_executor_that_fails_health_checks_and_destroy_it(
        self,
    ):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatKillsCurrentProcessIn10SecsAfterInitialization",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            create_test_graph_with_function(
                FunctionThatKillsCurrentProcessIn10SecsAfterInitialization
            )
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_idle_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE
            )

        # First the FE will be created successfully.
        await wait_condition(function_executor_has_idle_status)

        def function_executor_has_unhealthy_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNHEALTHY
            )

        # Health checks are done periodically and this one will timeout first so give a generous timeout.
        await wait_condition(function_executor_has_unhealthy_status, timeout_sec=60)
        self.assertTrue(function_executor_has_unhealthy_status())

        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)
        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )

        # Verify that Executor destroyed the unhealthy FE to release its resources asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Ask reconciler to delete the FE.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_idle_function_executors_and_destroy_them(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                    FunctionExecutorDescription(
                        id="fe-2",
                        namespace="test-namespace-2",
                        graph_name="test-graph-2",
                        graph_version="test-version-2",
                        function_name="function_b",
                    ),
                    FunctionExecutorDescription(
                        id="fe-3",
                        namespace="test-namespace-3",
                        graph_name="test-graph-3",
                        graph_version="test-version-3",
                        function_name="function_c",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = create_test_graph()

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executors_have_idle_statuses() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 3:
                return False
            for fe_state in latest_state.function_executor_states:
                fe_state: FunctionExecutorState
                if fe_state.description.id not in ["fe-1", "fe-2", "fe-3"]:
                    return False
                if (
                    fe_state.status
                    != FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE
                ):
                    return False
            return True

        await wait_condition(function_executors_have_idle_statuses)
        self.assertTrue(function_executors_have_idle_statuses())

        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )
        self.assertEqual(len(reported_states[-1].function_executor_states), 3)
        self.assertEqual(len(self.function_executor_states._states), 3)

        # Verify that FEs actually exist.
        async for executor_fe_state in self.function_executor_states:
            self.assertIsNotNone(executor_fe_state.function_executor)

        # Ask reconciler to delete the FE.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-2",
            graph_name="test-graph-2",
            graph_version="test-version-2",
            logger=unittest.mock.ANY,
        )
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-3",
            graph_name="test-graph-3",
            graph_version="test-version-3",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 3)
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_create_function_executor_then_run_task(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = create_test_graph()

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_is_idle() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            return (
                latest_state.function_executor_states[0].status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE
            )

        await wait_condition(function_executor_is_idle)
        self.assertTrue(function_executor_is_idle())

        self.assertLessEqual(
            reported_states[-1].server_clock, self.desired_states_generator.clock()
        )
        self.assertEqual(len(reported_states[-1].function_executor_states), 1)
        self.assertEqual(len(self.function_executor_states._states), 1)

        # Verify that FEs actually exist.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNotNone(executor_fe_state.function_executor)

        # Run a task on the FE.
        self.mock_downloader.download_input.return_value = create_test_input(
            "test-input-function-a"
        )
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        def task_outcome_reported() -> bool:
            return self.mock_task_reporter.report.call_count > 0

        await wait_condition(task_outcome_reported)
        self.assertTrue(task_outcome_reported())

        # Ask reconciler to delete the FE and forget the finished task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)

        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)

        self.assertEqual(self.mock_task_reporter.report.call_count, 1)
        self.assertIn("output", self.mock_task_reporter.report.call_args.kwargs)
        output: TaskOutput = self.mock_task_reporter.report.call_args.kwargs["output"]
        self.assertTrue(output.success)
        outputs: List[Any] = self.deserialize_function_output(output.function_output)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0], "function_a: test-input-function-a")

    async def test_create_function_executor_and_task_allocation_in_the_same_desired_state(
        self,
    ):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        self.mock_downloader.download_graph.return_value = create_test_graph()
        self.mock_downloader.download_input.return_value = create_test_input(
            "test-input-function-a"
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_is_idle_or_running_task() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            return latest_state.function_executor_states[0].status in [
                FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_IDLE,
                FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK,
            ]

        await wait_condition(function_executor_is_idle_or_running_task)
        self.assertTrue(function_executor_is_idle_or_running_task())

        # Finish the task on the FE.
        def task_outcome_reported() -> bool:
            return self.mock_task_reporter.report.call_count > 0

        await wait_condition(task_outcome_reported)
        self.assertTrue(task_outcome_reported())

        # Ask reconciler to delete the FE and forget the finished task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)

        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)

        self.assertEqual(self.mock_task_reporter.report.call_count, 1)
        self.assertIn("output", self.mock_task_reporter.report.call_args.kwargs)
        output: TaskOutput = self.mock_task_reporter.report.call_args.kwargs["output"]
        self.assertTrue(output.success)
        outputs: List[Any] = self.deserialize_function_output(output.function_output)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0], "function_a: test-input-function-a")

    async def test_run_task_on_function_executor_that_failed_to_startup(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            SerializedObject()
        )  # malformed graph results in FE startup failure (platform error).

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_platform_error_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_STARTUP_FAILED_PLATFORM_ERROR
            )

        await wait_condition(function_executor_has_platform_error_status)
        self.assertTrue(function_executor_has_platform_error_status())

        # Wait for more time to make sure that the task is stuck cause no FE to run on and it's not doing anything.
        await asyncio.sleep(5)
        # Check task is stuck and didn't get reported to Server.
        self.mock_task_reporter.report.assert_not_called()

        # Ask reconciler to delete the FE and cancel the stuck task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        # Verify that task input still got prefetched while waiting for FE.
        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)
        # Verify that still no outcome reported after explicit cancellation.
        self.mock_task_reporter.report.assert_not_called()

    async def test_run_task_on_unhealthy_function_executor(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatKillsCurrentProcessIn10SecsAfterInitialization",
                    ),
                ],
                task_allocations=[],
            )
        )

        self.mock_downloader.download_graph.return_value = (
            create_test_graph_with_function(
                FunctionThatKillsCurrentProcessIn10SecsAfterInitialization
            )
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_has_unhealthy_status() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            fe_state: FunctionExecutorState = latest_state.function_executor_states[0]
            if fe_state.description.id != "fe-1":
                return False
            return (
                fe_state.status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNHEALTHY
            )

        # Health checks are done periodically and this one will timeout first so give a generous timeout.
        await wait_condition(function_executor_has_unhealthy_status, timeout_sec=60)
        self.assertTrue(function_executor_has_unhealthy_status())

        # Verify that Executor destroyed the unhealthy FE to release its resources asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Allocate the task on the unhealthy FE.
        self.mock_downloader.download_input.return_value = create_test_input("whatever")
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="FunctionThatKillsCurrentProcessIn10SecsAfterInitialization",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="FunctionThatKillsCurrentProcessIn10SecsAfterInitialization",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        # Wait for more time to make sure that the task is stuck cause no FE to run on and it's not doing anything.
        await asyncio.sleep(5)
        # Check task is stuck and didn't get reported to Server.
        self.mock_task_reporter.report.assert_not_called()

        # Ask reconciler to delete the FE and cancel the stuck task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)
        # Verify that task input still got prefetched while waiting for FE.
        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)
        # Verify that still no outcome reported after explicit cancellation.
        self.mock_task_reporter.report.assert_not_called()

    async def test_run_task_on_function_executor_that_doesnt_exist(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        # Wait to make sure that reconciler ignores the task.
        await asyncio.sleep(10)
        # Verify expected reported state - nothing happened.
        self.assertEqual(len(reported_states[-1].function_executor_states), 0)
        # Verify expected reconciler state - nothing happened.
        self.assertEqual(len(self.state_reconciler._function_executor_controllers), 0)
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_not_called()
        self.mock_downloader.download_input.assert_not_called()
        self.mock_task_reporter.report.assert_not_called()

    async def test_task_timeout(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                            timeout_ms=5000,  # 5 seconds
                        ),
                    )
                ],
            )
        )

        self.mock_downloader.download_graph.return_value = create_test_graph()
        self.mock_downloader.download_input.return_value = create_test_input(
            "sleep_forever"
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_is_unhealthy() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            return (
                latest_state.function_executor_states[0].status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNHEALTHY
            )

        await wait_condition(function_executor_is_unhealthy)
        self.assertTrue(function_executor_is_unhealthy())

        # Verify that Executor destroyed the FE where the task timed out to stop running it and release FE resource asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Wait until task outcome reported.
        def task_outcome_reported() -> bool:
            return self.mock_task_reporter.report.call_count > 0

        await wait_condition(task_outcome_reported)
        self.assertTrue(task_outcome_reported())

        # Ask reconciler to delete the FE and forget the finished task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[],
                task_allocations=[],
            )
        )

        def no_function_executors_reported() -> bool:
            return len(reported_states[-1].function_executor_states) == 0

        await wait_condition(no_function_executors_reported)
        self.assertTrue(no_function_executors_reported())
        self.assertEqual(len(self.function_executor_states._states), 0)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)

        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)

        self.assertEqual(self.mock_task_reporter.report.call_count, 1)
        self.assertIn("output", self.mock_task_reporter.report.call_args.kwargs)
        output: TaskOutput = self.mock_task_reporter.report.call_args.kwargs["output"]
        self.assertFalse(output.success)
        self.assertEqual(
            output.stderr, "Function exceeded its configured timeout of 5.000 sec."
        )

    async def test_cancel_running_task(self):
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[
                    TaskAllocation(
                        function_executor_id="fe-1",
                        task=Task(
                            id="task-1",
                            namespace="test-namespace-1",
                            graph_name="test-graph-1",
                            graph_version="test-version-1",
                            function_name="function_a",
                            graph_invocation_id="test-graph-invocation",
                            input_key="test-input-key",
                        ),
                    )
                ],
            )
        )

        self.mock_downloader.download_graph.return_value = create_test_graph()
        self.mock_downloader.download_input.return_value = create_test_input(
            "sleep_forever"
        )

        reported_states: List[ExecutorState] = []
        self.create_executor_api_server_test_scenario(
            self.desired_states_generator, reported_states
        )

        def function_executor_is_running_task() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            return (
                latest_state.function_executor_states[0].status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_RUNNING_TASK
            )

        await wait_condition(function_executor_is_running_task)
        self.assertTrue(function_executor_is_running_task())

        # Ask reconciler to cancel the running task.
        self.desired_states_generator.set_desired_state(
            DesiredExecutorState(
                function_executors=[
                    FunctionExecutorDescription(
                        id="fe-1",
                        namespace="test-namespace-1",
                        graph_name="test-graph-1",
                        graph_version="test-version-1",
                        function_name="function_a",
                    ),
                ],
                task_allocations=[],
            )
        )

        # Reconciler should mark FE unhealthy to cancel the running task
        # and destroy the FE to release its resources asap and stop the running task.
        def function_executor_is_unhealthy() -> bool:
            if len(reported_states) == 0:
                return False
            latest_state: ExecutorState = reported_states[-1]
            if len(latest_state.function_executor_states) != 1:
                return False
            return (
                latest_state.function_executor_states[0].status
                == FunctionExecutorStatus.FUNCTION_EXECUTOR_STATUS_UNHEALTHY
            )

        await wait_condition(function_executor_is_unhealthy)
        self.assertTrue(function_executor_is_unhealthy())

        # Verify that Executor destroyed the FE where the task timed out to stop running it and release FE resource asap.
        executor_fe_state = await self.function_executor_states.get("fe-1")
        self.assertIsNone(executor_fe_state.function_executor)

        # Verify expected mock calls.
        self.mock_downloader.download_graph.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_version="test-version-1",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_graph.call_count, 1)

        self.mock_downloader.download_input.assert_any_call(
            namespace="test-namespace-1",
            graph_name="test-graph-1",
            graph_invocation_id="test-graph-invocation",
            input_key="test-input-key",
            logger=unittest.mock.ANY,
        )
        self.assertEqual(self.mock_downloader.download_input.call_count, 1)

        # The cancelled task outcome should never be reported to Server.
        self.mock_task_reporter.report.assert_not_called()


if __name__ == "__main__":
    unittest.main()
