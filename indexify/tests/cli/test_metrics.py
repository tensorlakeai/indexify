import unittest
from typing import Dict

import httpx

# We're using internal APIs here, this might break when we update prometheus_client.
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample


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


if __name__ == "__main__":
    unittest.main()
