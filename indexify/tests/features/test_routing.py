import unittest
from typing import List, Union

from parameterized import parameterized_class
from tensorlake import Graph, RemoteGraph, RouteTo, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path


def remote_or_local_graph(graph, remote=True) -> Union[RemoteGraph, Graph]:
    if remote:
        return RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))
    return graph


def test_graph_name(test_case: unittest.TestCase) -> str:
    """Converts a test case to a unique graph name.

    Example:
    >>> class TestGraphReduce(unittest.TestCase):
    ...     def test_simple(self):
    ...         g = Graph(name=graph_name(self), start_node=generate_seq)
    ...         # ...
    ...         print(g.name)
    ...         # test_graph_reduce_test_simple
    """
    return unittest.TestCase.id(test_case).replace(".", "_")


@tensorlake_function()
def a(value: int) -> int:
    return value + 3


@tensorlake_function()
def b(value: int) -> int:
    return value + 4


@tensorlake_function()
def c(value: int) -> int:
    return value + 5


@tensorlake_function(next=[a, b])
def fan_out(value: int) -> int:
    return value


@tensorlake_function(next=[a, b, c])
def route_out(value: int) -> RouteTo[int, Union[a, b, c]]:
    if value % 2 == 0:
        return RouteTo(value + 1, [a])

    return RouteTo(value + 2, [b, c])


@tensorlake_function(accumulate=int)
def sum_of_squares(current: int, value: int) -> int:
    return current + value


@tensorlake_function(next=sum_of_squares)
def square_values(value: int) -> int:
    return value * value


@tensorlake_function(next=square_values)
def parallel_map(count: int) -> List[int]:
    return list(range(count))


@tensorlake_function()
def should_not_run(x: str) -> str:
    return x + "c"


@tensorlake_function(next=should_not_run)
def simple_success(x: str) -> RouteTo[str, should_not_run]:
    return RouteTo(x + "b", [])


def _get_class_name(cls, unused_num, param_dict):
    return f"{cls.__name__}_{'remote' if param_dict['is_remote'] else 'local'}"


@parameterized_class(
    ("is_remote"),
    [
        (False,),
        (True,),
    ],
    class_name_func=_get_class_name,
)
class TestRouting(unittest.TestCase):
    def test_fan_out(self):
        graph = Graph(name=test_graph_name(self), start_node=fan_out)
        graph = remote_or_local_graph(graph, self.is_remote)
        inv = graph.run(block_until_done=True, value=3)
        a_out = graph.output(inv, "a")
        b_out = graph.output(inv, "b")

        self.assertEqual(6, a_out[0])  # 6 == 3 + 3
        self.assertEqual(7, b_out[0])  # 7 == 3 + 4

    def test_route_out(self):
        graph = Graph(name=test_graph_name(self), start_node=route_out)
        graph = remote_or_local_graph(graph, self.is_remote)
        inv = graph.run(block_until_done=True, value=3)
        a_out = graph.output(inv, "a")
        b_out = graph.output(inv, "b")
        c_out = graph.output(inv, "c")

        # Verify graph outputs.
        self.assertEqual([], a_out)
        self.assertEqual(9, b_out[0])  # 7 == 3 + 2 + 4
        self.assertEqual(10, c_out[0])  # 8 == 3 + 2 + 5

    def test_parallel_map(self):
        graph = Graph(name=test_graph_name(self), start_node=parallel_map)
        graph = remote_or_local_graph(graph, self.is_remote)
        inv = graph.run(block_until_done=True, count=3)
        sum_out = graph.output(inv, "sum_of_squares")

        # Verify graph outputs.
        self.assertEqual(5, sum_out[0])  # 0^2 + 1^2 + 2^2

    def test_early_success(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test for early graph success",
            start_node=simple_success,
        )
        graph = remote_or_local_graph(graph, self.is_remote)
        invocation_id = graph.run(block_until_done=True, x="a")
        output_simple_success = graph.output(invocation_id, "simple_success")
        output_should_not_run = graph.output(invocation_id, "should_not_run")
        self.assertEqual(output_simple_success, ["ab"])
        self.assertEqual(output_should_not_run, [])


if __name__ == "__main__":
    unittest.main()
