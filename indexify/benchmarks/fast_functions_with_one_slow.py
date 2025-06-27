# The benchmark runs a scenario when there's a few functions in the workflow
# with all but one being fast.
import argparse
import time
from typing import Any, List

from tensorlake import Graph, RemoteGraph, TensorlakeCompute
from tensorlake.error import GraphStillProcessing
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path

# Simulate a simple CPU function with a small container image.
_FAST_COLD_START_DURATION_SEC = 2
# Simulate model download and loading it into GPU.
_SLOW_COLD_START_DURATION_SEC = 10

# Duration of fast function run.
_FAST_FUNCTION_DURATION_SEC = 0.1
# Duration of slow function run.
_SLOW_FUNCTION_DURATION_SEC = 10


class FastFunctionWithFastColdStart_1(TensorlakeCompute):
    name = "fast_function_with_fast_cold_start_1"

    def __init__(self):
        time.sleep(_FAST_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_FAST_FUNCTION_DURATION_SEC)
        return self.name


class FastFunctionWithFastColdStart_2(TensorlakeCompute):
    name = "fast_function_with_fast_cold_start_2"

    def __init__(self):
        time.sleep(_FAST_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_FAST_FUNCTION_DURATION_SEC)
        return self.name


class FastFunctionWithFastColdStart_3(TensorlakeCompute):
    name = "fast_function_with_fast_cold_start_3"

    def __init__(self):
        time.sleep(_FAST_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_FAST_FUNCTION_DURATION_SEC)
        return self.name


class FastFunctionWithSlowColdStart(TensorlakeCompute):
    name = "fast_function_with_slow_cold_start"

    def __init__(self):
        time.sleep(_SLOW_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_FAST_FUNCTION_DURATION_SEC)
        return self.name


class SlowFunctionWithFastColdStart(TensorlakeCompute):
    name = "slow_function_with_fast_cold_start"

    def __init__(self):
        time.sleep(_FAST_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_SLOW_FUNCTION_DURATION_SEC)
        return self.name


class SlowFunctionWithSlowColdStart(TensorlakeCompute):
    name = "slow_function_with_slow_cold_start"

    def __init__(self):
        time.sleep(_SLOW_COLD_START_DURATION_SEC)

    def run(self, x: str) -> str:
        time.sleep(_SLOW_FUNCTION_DURATION_SEC)
        return self.name


def wait_function_output(
    graph: RemoteGraph, invocation_id: str, func_name: str
) -> List[Any]:
    while True:
        try:
            print(
                f"Waiting for output of graph: {graph._name} function '{func_name}' with invocation ID '{invocation_id}'"
            )
            return graph.output(invocation_id, func_name)
        except GraphStillProcessing:
            time.sleep(1)


def deploy_graph() -> RemoteGraph:
    graph = Graph(
        name="benchmark_fast_functions_with_one_slow",
        start_node=FastFunctionWithFastColdStart_1,
        # start_node=SlowFunctionWithSlowColdStart,
    )
    graph.add_edge(FastFunctionWithFastColdStart_1, FastFunctionWithFastColdStart_2)
    graph.add_edge(FastFunctionWithFastColdStart_2, SlowFunctionWithSlowColdStart)
    graph.add_edge(SlowFunctionWithSlowColdStart, FastFunctionWithFastColdStart_3)

    return RemoteGraph.deploy(graph, code_dir_path=graph_code_dir_path(__file__))


def benchmark(g: RemoteGraph, invocations_count: int) -> None:
    """Run the graph and measure the time taken."""
    start_time = time.time()
    invocation_ids: List[str] = []
    for _ in range(invocations_count):
        invocation_id: str = g.run(block_until_done=False, x="test")
        invocation_ids.append(invocation_id)

    for invocation_id in invocation_ids:
        last_function_name = FastFunctionWithFastColdStart_3.name
        # last_function_name = SlowFunctionWithSlowColdStart.name
        output = wait_function_output(g, invocation_id, last_function_name)
        if output != [last_function_name]:
            raise ValueError(
                f"Unexpected output for invocation {invocation_id}: {output}"
            )

    end_time = time.time()
    print(f"Graph run completed in {end_time - start_time:.2f} seconds.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark fast functions with one slow function."
    )
    parser.add_argument(
        "--invocations",
        type=int,
        default=30,
        help="Number of benchmark invocations (default: 30)",
    )
    args = parser.parse_args()

    g: RemoteGraph = deploy_graph()
    benchmark(g, invocations_count=args.invocations)
