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

    def test_function_fails_after_exhausting_retries(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_aways_fails,
        )
        graph = remote_or_local_graph(graph, remote=True)
        invocation_id = graph.run(block_until_done=True, x=1)

        outputs = graph.output(invocation_id, function_aways_fails.name)
        self.assertEqual(len(outputs), 0)


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


def unfail_constructor_with_delay():
    time.sleep(5)
    FunctionWithFailingConstructor.unfail_constructor()


@unittest.skip("Function Executor startup retries feature has a bug")
class TestFunctionConstructorRetries(unittest.TestCase):
    def test_function_constructor_succeeds_after_two_retries(self):
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


if __name__ == "__main__":
    unittest.main()
