import io
import time
import unittest
from contextlib import redirect_stdout
from typing import List, Union

from tensorlake import (
    Graph,
    RemoteGraph,
    TensorlakeCompute,
    TensorlakeRouter,
    tensorlake_function,
    tensorlake_router,
)
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from testing import test_graph_name


class FunctionThatSleepsForeverOnInitialization(TensorlakeCompute):
    name = "FunctionThatSleepsForeverOnInitialization"
    timeout = 2

    def __init__(self):
        super().__init__()
        time.sleep(1000000)

    def run(self, action: str) -> str:
        raise Exception("This function can never run because it fails to initialize.")


@tensorlake_function(timeout=3)
def function_that_sleeps_forever_when_running() -> str:
    time.sleep(1000000)
    return "success"


class TestFunctionTimeouts(unittest.TestCase):
    def test_initilization(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=FunctionThatSleepsForeverOnInitialization,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function initialization didn't timeout in duration close to 2 sec",
        )
        # Check that the function failed.
        output = graph.output(
            invocation_id, "FunctionThatSleepsForeverOnInitialization"
        )
        self.assertEqual(len(output), 0)

    def test_run(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_that_sleeps_forever_when_running,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        # We don't have a public SDK API to read a functions' stderr
        # so we rely on internal SDK behavior where it prints a failed function's
        # stderr to the current stdout.
        sdk_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(sdk_stdout):
            invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Function run didn't timeout in duration close to 3 sec",
        )
        # Check that the function failed.
        output = graph.output(
            invocation_id, "function_that_sleeps_forever_when_running"
        )
        self.assertEqual(len(output), 0)

        # Use regex to ignore console formatting characters
        self.assertRegex(
            sdk_stdout.getvalue(),
            r"Function exceeded its configured timeout of.*3.000.*sec",
        )


class RouterThatSleepsForeverOnInitialization(TensorlakeRouter):
    name = "RouterThatSleepsForeverOnInitialization"
    timeout = 1

    def __init__(self):
        super().__init__()
        time.sleep(1000000)

    def run(self, action: str) -> List[
        Union[
            FunctionThatSleepsForeverOnInitialization,
            function_that_sleeps_forever_when_running,
        ]
    ]:
        raise Exception("This router can never run because it fails to initialize.")


@tensorlake_router(timeout=2)
def router_that_sleeps_forever_when_running() -> Union[
    FunctionThatSleepsForeverOnInitialization,
    function_that_sleeps_forever_when_running,
]:
    time.sleep(1000000)
    return function_that_sleeps_forever_when_running


class TestRouterTimeouts(unittest.TestCase):
    def test_initilization(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=RouterThatSleepsForeverOnInitialization,
        )
        graph.route(
            RouterThatSleepsForeverOnInitialization,
            [
                FunctionThatSleepsForeverOnInitialization,
                function_that_sleeps_forever_when_running,
            ],
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Router initialization didn't timeout in duration close to 1 sec",
        )

    def test_run(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=router_that_sleeps_forever_when_running,
        )
        graph.route(
            router_that_sleeps_forever_when_running,
            [
                FunctionThatSleepsForeverOnInitialization,
                function_that_sleeps_forever_when_running,
            ],
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )
        start_time = time.monotonic()
        # We don't have a public SDK API to read a functions' stderr
        # so we rely on internal SDK behavior where it prints a failed function's
        # stderr to the current stdout.
        sdk_stdout: io.StringIO = io.StringIO()
        with redirect_stdout(sdk_stdout):
            invocation_id = graph.run(block_until_done=True)
        duration_sec = time.monotonic() - start_time
        self.assertLess(
            duration_sec,
            20,  # Add extra for state reporting and reconciliation latency
            "Router run didn't timeout in duration close to 2 sec",
        )

        # Use regex to ignore console formatting characters
        self.assertRegex(
            sdk_stdout.getvalue(),
            r"Function exceeded its configured timeout of.*2.000.*sec",
        )


if __name__ == "__main__":
    unittest.main()
