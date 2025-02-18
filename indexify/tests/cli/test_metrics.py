import subprocess
import unittest
from typing import Dict

import httpx

# We're using internal APIs here, this might break when we update prometheus_client.
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample
from testing import ExecutorProcessContextManager, wait_executor_startup


class TestMetrics(unittest.TestCase):
    def test_cli_package(self):
        response = httpx.get(f"http://localhost:7000/monitoring/metrics")
        self.assertEqual(response.status_code, 200)
        metrics: Dict[str, Metric] = {}
        for metric in text_string_to_metric_families(response.text):
            metrics[metric.name] = metric

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

            response = httpx.get(f"http://localhost:7001/monitoring/metrics")
            self.assertEqual(response.status_code, 200)
            metrics: Dict[str, Metric] = {}
            for metric in text_string_to_metric_families(response.text):
                metrics[metric.name] = metric

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


if __name__ == "__main__":
    unittest.main()
