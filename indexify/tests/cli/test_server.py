import subprocess
import time
import unittest
from typing import List

import testing
from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph
from testing import (
    ExecutorProcessContextManager,
    executor_pid,
    test_graph_name,
    wait_executor_startup,
)


@tensorlake_function()
def get_executor_pid(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return executor_pid()


@tensorlake_function()
def success_func(sleep_secs: float) -> str:
    time.sleep(sleep_secs)
    return "success"


class TestServer(unittest.TestCase):
    def test_server_distributes_invocations_fairly_between_two_executors(self):
        print(
            "Waiting for 10 seconds for Server to notice that any previously existing Executors exited."
        )
        time.sleep(10)

        with ExecutorProcessContextManager(
            [
                "--dev",
                "--ports",
                "60000",
                "60001",
                "--monitoring-server-port",
                "7001",
            ],
            keep_std_outputs=False,
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            graph = Graph(
                name=test_graph_name(self),
                description="test",
                start_node=get_executor_pid,
            )
            graph = RemoteGraph.deploy(graph, additional_modules=[testing])

            invocations_per_pid = {}
            invocation_ids: List[str] = []
            # Run many invokes to collect enough samples.
            for _ in range(200):
                invocation_id = graph.run(block_until_done=False, sleep_secs=0)
                invocation_ids.append(invocation_id)

            # Let all the invocations finish in at least (0.02 sec * 200) = 4 seconds + 1 sec
            # for any overheads.
            print("Waiting 5 secs for all invocations to finish.")
            time.sleep(5)

            for invocation_id in invocation_ids:
                output = graph.output(invocation_id, "get_executor_pid")
                self.assertEqual(len(output), 1)
                executor_pid = output[0]
                if executor_pid not in invocations_per_pid:
                    invocations_per_pid[executor_pid] = 0
                invocations_per_pid[executor_pid] += 1

            for pid, invocations_count in invocations_per_pid.items():
                print(f"Executor PID: {pid}, invocations count:{invocations_count}")

            for _, invocations_count in invocations_per_pid.items():
                # Allow +-25 invocations difference between the executors.
                self.assertGreater(invocations_count, 75)
                self.assertLess(invocations_count, 125)

    def test_server_redistributes_invocations_when_new_executor_joins(self):
        print(
            "Waiting for 10 seconds for Server to notice that any previously existing Executors exited."
        )
        time.sleep(10)

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=get_executor_pid,
        )
        graph = RemoteGraph.deploy(graph, additional_modules=[testing])

        invocations_per_pid = {}
        invocation_ids: List[str] = []
        # Run many invokes to collect enough samples.
        for _ in range(200):
            invocation_id = graph.run(block_until_done=False, sleep_secs=0.1)
            invocation_ids.append(invocation_id)

        with ExecutorProcessContextManager(
            [
                "--dev",
                "--ports",
                "60000",
                "60001",
                "--monitoring-server-port",
                "7001",
            ],
            keep_std_outputs=False,
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)

            # Let all the invocations finish in at least (0.1 sec * 200) = 20 seconds + 10 sec
            # for any overheads.
            print("Waiting 30 secs for all invocations to finish.")
            time.sleep(30)

            for invocation_id in invocation_ids:
                output = graph.output(invocation_id, "get_executor_pid")
                self.assertEqual(len(output), 1)
                executor_pid = output[0]
                if executor_pid not in invocations_per_pid:
                    invocations_per_pid[executor_pid] = 0
                invocations_per_pid[executor_pid] += 1

            for pid, invocations_count in invocations_per_pid.items():
                print(f"Executor PID: {pid}, invocations count:{invocations_count}")

            for _, invocations_count in invocations_per_pid.items():
                # At least 25% to 75% of all tasks should go to each executor after the new executor joins.
                self.assertGreater(invocations_count, 50)
                self.assertLess(invocations_count, 150)

    # def test_all_tasks_succeed_when_executor_exits(self):
    #     print(
    #         "Waiting for 10 seconds for Server to notice that any previously existing Executors exited."
    #     )
    #     time.sleep(10)

    #     with ExecutorProcessContextManager(
    #         [
    #             "--dev",
    #             "--ports",
    #             "60000",
    #             "60001",
    #             "--monitoring-server-port",
    #             "7001",
    #         ],
    #         keep_std_outputs=False,
    #     ) as executor_a:
    #         executor_a: subprocess.Popen
    #         print(f"Started Executor A with PID: {executor_a.pid}")
    #         wait_executor_startup(7001)

    #         graph = Graph(
    #             name=test_graph_name(self),
    #             description="test",
    #             start_node=success_func,
    #         )
    #         graph = RemoteGraph.deploy(graph, additional_modules=[testing])

    #         invocation_ids: List[str] = []
    #         # Run many invokes to collect enough samples.
    #         for _ in range(200):
    #             invocation_id = graph.run(block_until_done=False, sleep_secs=0.1)
    #             invocation_ids.append(invocation_id)

    #     # Let all the invocations finish in at least (0.1 sec * 200) = 20 seconds + 10 sec
    #     # for any overheads.
    #     print("Waiting 30 secs for all invocations to finish.")
    #     time.sleep(30)

    #     for invocation_id in invocation_ids:
    #         output = graph.output(invocation_id, "success_func")
    #         self.assertEqual(len(output), 1)
    #         self.assertEqual(output[0], "success")


if __name__ == "__main__":
    unittest.main()
