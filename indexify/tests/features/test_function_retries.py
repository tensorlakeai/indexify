import os
import threading
import time
import unittest

from tensorlake import (
    Graph,
    TensorlakeCompute,
    tensorlake_function,
)
from tensorlake.functions_sdk.retries import Retries
from testing import remote_or_local_graph, test_graph_name


@tensorlake_function(retries=Retries(max_retries=3, max_delay=1.0))
def function_succeeds_after_two_retries(x: int) -> str:
    function_succeeds_after_two_retries.call_number += 1

    if function_succeeds_after_two_retries.call_number == 4:
        return "success"
    else:
        raise Exception("Function failed, please retry")


function_succeeds_after_two_retries.call_number = 0


@tensorlake_function(retries=Retries(max_retries=3, max_delay=1.0))
def function_aways_fails(x: int) -> str:
    raise Exception("Function failed and will never succeed")


@tensorlake_function(retries=Retries(max_retries=3, max_delay=1.0), timeout=1)
def function_always_times_out(x: int) -> str:
    with open(function_always_times_out.FILE_PATH, "a") as f:
        f.write("executed\n")
    time.sleep(1000)


function_always_times_out.FILE_PATH = "/tmp/function_always_times_out_counter"


class TestFunctionRetries(unittest.TestCase):
    def test_function_succeeds_after_two_retries(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_succeeds_after_two_retries,
        )
        graph = remote_or_local_graph(graph, remote=True)
        invocation_id = graph.run(block_until_done=True, x=1)

        outputs = graph.output(invocation_id, function_succeeds_after_two_retries.name)
        self.assertEqual(outputs, ["success"])

    def test_function_fails_after_exhausting_failure_retries(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_aways_fails,
        )
        graph = remote_or_local_graph(graph, remote=True)
        invocation_id = graph.run(block_until_done=True, x=1)

        outputs = graph.output(invocation_id, function_aways_fails.name)
        self.assertEqual(len(outputs), 0)

    @unittest.skip("Function timeout retries feature has a bug")
    def test_function_fails_after_exhausting_timeout_retries(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_always_times_out,
        )
        graph = remote_or_local_graph(graph, remote=True)
        if os.path.exists(function_always_times_out.FILE_PATH):
            os.remove(function_always_times_out.FILE_PATH)
        invocation_id = graph.run(block_until_done=True, x=1)

        outputs = graph.output(invocation_id, function_always_times_out.name)
        self.assertEqual(len(outputs), 0)
        with open(function_always_times_out.FILE_PATH, "r") as f:
            lines = f.readlines()
            self.assertEqual(lines, ["executed\n"] * 4)  # 3 retries + initial call


class FunctionWithFailingConstructor(TensorlakeCompute):
    name = "FunctionWithFailingConstructor"
    FILE_PATH = "/tmp/FunctionWithFailingConstructor_fail"

    def __init__(self):
        super().__init__()
        if os.path.exists(self.FILE_PATH):
            raise Exception("Constructor failed")

    def run(self, x: int) -> str:
        return "success"

    @classmethod
    def unfail_constructor(cls):
        os.remove(cls.FILE_PATH)

    @classmethod
    def fail_constructor(cls):
        with open(cls.FILE_PATH, "w") as f:
            f.write(
                "This file is used to fail the constructor of FunctionWithFailingConstructor."
            )


class FunctionWithTimingOutConstructor(TensorlakeCompute):
    name = "FunctionWithTimingOutConstructor"
    timeout = 1
    FILE_PATH = "/tmp/FunctionWithTimingOutConstructor_timeout"

    def __init__(self):
        super().__init__()
        if os.path.exists(self.FILE_PATH):
            time.sleep(1000)

    def run(self, x: int) -> str:
        return "success"

    @classmethod
    def untimeout_constructor(cls):
        os.remove(cls.FILE_PATH)

    @classmethod
    def timeout_constructor(cls):
        with open(cls.FILE_PATH, "w") as f:
            f.write(
                "This file is used to timeout the constructor of FunctionWithTimingOutConstructor."
            )


@unittest.skip("Function Executor startup retries feature has a bug")
class TestFunctionConstructorRetries(unittest.TestCase):
    def test_function_constructor_succeeds_after_failing_for_5_secs(self):
        def unfail_constructor_with_delay():
            time.sleep(5)
            FunctionWithFailingConstructor.unfail_constructor()

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=FunctionWithFailingConstructor,
        )
        graph = remote_or_local_graph(graph, remote=True)
        FunctionWithFailingConstructor.fail_constructor()
        threading.Thread(target=unfail_constructor_with_delay).start()
        invocation_id = graph.run(block_until_done=True, x=1)
        outputs = graph.output(invocation_id, FunctionWithFailingConstructor.name)
        self.assertEqual(outputs, ["success"])

    def test_function_constructor_succeeds_after_timing_out_for_5_secs(self):
        def untimeout_constructor_with_delay():
            time.sleep(5)
            FunctionWithTimingOutConstructor.untimeout_constructor()

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=FunctionWithTimingOutConstructor,
        )
        graph = remote_or_local_graph(graph, remote=True)
        FunctionWithTimingOutConstructor.timeout_constructor()
        threading.Thread(target=untimeout_constructor_with_delay).start()
        invocation_id = graph.run(block_until_done=True, x=1)
        outputs = graph.output(invocation_id, FunctionWithTimingOutConstructor.name)
        self.assertEqual(outputs, ["success"])


if __name__ == "__main__":
    unittest.main()
