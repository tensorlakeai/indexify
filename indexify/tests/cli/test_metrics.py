import importlib.metadata
import subprocess
import sys
import unittest
from typing import Dict

import httpx

# We're using internal APIs here, this might break when we update prometheus_client.
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample
from tensorlake import Graph, RemoteGraph, tensorlake_function
from testing import (
    ExecutorProcessContextManager,
    test_graph_name,
    wait_executor_startup,
)


def fetch_metrics(
    test_case: unittest.TestCase, monitoring_port: int = 7000
) -> Dict[str, Metric]:
    response = httpx.get(f"http://localhost:{monitoring_port}/monitoring/metrics")
    test_case.assertEqual(response.status_code, 200)
    metrics: Dict[str, Metric] = {}
    for metric in text_string_to_metric_families(response.text):
        metrics[metric.name] = metric
    return metrics


@tensorlake_function()
def successful_function(arg: str) -> str:
    return "success"


class TestMetrics(unittest.TestCase):
    def test_cli_package(self):
        metrics: Dict[str, Metric] = fetch_metrics(self)

        self.assertIn("cli_info", metrics)
        cli_info_metric: Metric = metrics["cli_info"]
        self.assertEqual(len(cli_info_metric.samples), 1)
        cli_info_sample: Sample = cli_info_metric.samples[0]
        self.assertEqual(cli_info_sample.labels, {"package": "indexify"})
        self.assertEqual(cli_info_sample.value, 1.0)

    def test_executor_id_argument_valid_characters(self):
        with ExecutorProcessContextManager(
            [
                "--dev",
                "--ports",
                "60000",
                "60001",
                "--monitoring-server-port",
                "7001",
                "--executor-id",
                "-test_executor_id",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            wait_executor_startup(7001)
            metrics: Dict[str, Metric] = fetch_metrics(self, monitoring_port=7001)

            self.assertIn("executor_info", metrics)
            info_metric: Metric = metrics["executor_info"]
            self.assertEqual(len(info_metric.samples), 1)
            info_sample: Sample = info_metric.samples[0]
            self.assertIn("id", info_sample.labels)
            self.assertEqual(info_sample.labels["id"], "-test_executor_id")

    def test_executor_id_argument_invalid_character(self):
        with ExecutorProcessContextManager(
            [
                "--dev",
                "--ports",
                "60001",
                "60002",
                "--monitoring-server-port",
                "7002",
                "--executor-id",
                "@-test_executor_id",
            ]
        ) as executor_a:
            executor_a: subprocess.Popen
            print(f"Started Executor A with PID: {executor_a.pid}")
            try:
                wait_executor_startup(7002)
                self.fail(
                    "Executor should not have started with the invalid executor ID."
                )
            except Exception:
                pass

    def test_expected_function_executor_infos(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=successful_function,
        )
        graph = RemoteGraph.deploy(graph)
        invocation_id = graph.run(
            block_until_done=True,
            arg="ignored",
        )
        output = graph.output(invocation_id, "successful_function")
        self.assertEqual(output, ["success"])

        metrics: Dict[str, Metric] = fetch_metrics(self)
        fe_infos_metric: Metric = metrics.get("function_executor_infos")
        self.assertEqual(len(fe_infos_metric.samples), 1)
        fe_info_sample: Sample = fe_infos_metric.samples[0]
        self.assertEqual(fe_info_sample.name, "function_executor_infos_total")
        # This assertion assumes that Subprocess Function Executors are used so
        # all the values for these labels are the same as values obtained in this test.
        self.assertEqual(
            fe_info_sample.labels,
            {
                "version": "0.1.0",
                "sdk_version": importlib.metadata.version("tensorlake"),
                "sdk_language": "python",
                "sdk_language_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            },
        )


if __name__ == "__main__":
    unittest.main()
