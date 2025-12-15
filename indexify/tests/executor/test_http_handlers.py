import unittest
from typing import Dict, List

import httpx

# We're using internal APIs here, this might break when we update prometheus_client.
from prometheus_client.metrics_core import Metric
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample
from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def successful_function_cold_start_1(arg: str) -> str:
    return "success"


@application()
@function()
def successful_function_cold_start_2(arg: str) -> str:
    return "success"


def fetch_metrics(test_case: unittest.TestCase) -> Dict[str, Metric]:
    response = httpx.get(f"http://localhost:7000/monitoring/metrics")
    test_case.assertEqual(response.status_code, 200)
    metrics: Dict[str, Metric] = {}
    for metric in text_string_to_metric_families(response.text):
        metrics[metric.name] = metric
    return metrics


def get_metric(
    test_case: unittest.TestCase, metrics: Dict[str, Metric], name: str
) -> Metric:
    if name in metrics:
        return metrics[name]
    test_case.fail(f"Metric {name} not found in the metrics")


def get_sample(
    test_case: unittest.TestCase,
    metrics: Dict[str, Metric],
    name: str,
    labels: Dict[str, str],
) -> Sample:
    for metric in metrics.values():
        for sample in metric.samples:
            if name == sample.name and labels == sample.labels:
                return sample
    test_case.fail(f"Sample {name} with labels {labels} not found in the metrics")


def assert_sample_exists(
    test_case: unittest.TestCase,
    metrics: Dict[str, Metric],
    name: str,
    labels: Dict[str, str] | None = None,
    value: float | None = None,
) -> None:
    for metric in metrics.values():
        for sample in metric.samples:
            if name != sample.name:
                continue
            if labels is not None and labels != sample.labels:
                continue
            if value is not None and value != sample.value:
                continue
            test_case.assertTrue(
                True,
                f"Sample {name} with labels {labels} and value {value} was found in the metrics",
            )
            return
    test_case.fail(
        f"Sample {name} with labels {labels} and value {value} not found in the metrics"
    )


class SampleSpec:
    def __init__(self, name: str, labels: Dict[str, str], value: float):
        self.name = name
        self.labels = labels
        self.value = value


class TestMetrics(unittest.TestCase):
    def test_all_expected_metrics_are_present(self):
        # See how metrics are mapped to their samples at https://prometheus.io/docs/concepts/metric_types/.
        expected_sample_names = [
            "python_info",
            # application downloads
            "application_downloads_total",
            "application_download_errors_total",
            "application_downloads_from_cache_total",
            "application_download_latency_seconds_count",
            "application_download_latency_seconds_sum",
            # blob store get metadata
            "blob_store_get_metadata_requests_total",
            "blob_store_get_metadata_errors_total",
            "blob_store_get_metadata_latency_seconds_count",
            # blob store presign uri
            "blob_store_presign_uri_requests_total",
            "blob_store_presign_uri_errors_total",
            "blob_store_presign_uri_latency_seconds_count",
            # blob store uploads
            "blob_store_upload_requests_total",
            "blob_store_upload_errors_total",
            "blob_store_upload_latency_seconds_count",
            # blob store create multipart upload
            "blob_store_create_multipart_upload_requests_total",
            "blob_store_create_multipart_upload_errors_total",
            "blob_store_create_multipart_upload_latency_seconds_count",
            # blob store complete multipart upload
            "blob_store_complete_multipart_upload_requests_total",
            "blob_store_complete_multipart_upload_errors_total",
            "blob_store_complete_multipart_upload_latency_seconds_count",
            # blob store abort multipart upload
            "blob_store_abort_multipart_upload_requests_total",
            "blob_store_abort_multipart_upload_errors_total",
            "blob_store_abort_multipart_upload_latency_seconds_count",
            # allocation preparation
            "allocation_preparations_total",
            "allocation_preparation_errors_total",
            "allocation_preparation_latency_seconds_count",
            "allocation_preparation_latency_seconds_sum",
            "allocations_getting_prepared",
            # FE health checker
            "function_executor_failed_health_checks_total",
            "function_executor_health_check_latency_seconds_count",
            "function_executor_health_check_latency_seconds_sum",
            # Function executor create/destroy.
            "function_executors_count",
            #
            "function_executor_creates_total",
            "function_executor_create_latency_seconds_count",
            "function_executor_create_latency_seconds_sum",
            "function_executor_create_errors_total",
            #
            "function_executor_destroys_total",
            "function_executor_destroy_latency_seconds_count",
            "function_executor_destroy_latency_seconds_sum",
            "function_executor_destroy_errors_total",
            #
            "function_executor_create_server_latency_seconds_count",
            "function_executor_create_server_latency_seconds_sum",
            "function_executor_create_server_errors_total",
            #
            "function_executor_destroy_server_latency_seconds_count",
            "function_executor_destroy_server_latency_seconds_sum",
            "function_executor_destroy_server_errors_total",
            #
            "function_executor_establish_channel_latency_seconds_count",
            "function_executor_establish_channel_latency_seconds_sum",
            "function_executor_establish_channel_errors_total",
            #
            "function_executor_destroy_channel_latency_seconds_count",
            "function_executor_destroy_channel_latency_seconds_sum",
            "function_executor_destroy_channel_errors_total",
            #
            "function_executor_get_info_rpc_latency_seconds_count",
            "function_executor_get_info_rpc_latency_seconds_sum",
            "function_executor_get_info_rpc_errors_total",
            #
            "function_executor_initialize_rpc_latency_seconds_count",
            "function_executor_initialize_rpc_latency_seconds_sum",
            "function_executor_initialize_rpc_errors_total",
            #
            "function_executor_create_health_checker_latency_seconds_count",
            "function_executor_create_health_checker_latency_seconds_sum",
            "function_executor_create_health_checker_errors_total",
            #
            "function_executor_destroy_health_checker_latency_seconds_count",
            "function_executor_destroy_health_checker_latency_seconds_sum",
            "function_executor_destroy_health_checker_errors_total",
            # FE states
            "function_executors_with_state",
            # Executor
            "executor_info",
            "executor_state",
            # allocation lifecycle steps
            "allocations_fetched_total",
            "allocations_completed_total",
            "allocation_completion_latency_seconds_count",
            "allocation_completion_latency_seconds_sum",
            # allocation finalization
            "allocation_finalizations_total",
            "allocation_finalization_errors_total",
            "allocation_finalization_latency_seconds_count",
            "allocation_finalization_latency_seconds_sum",
            # allocation scheduling
            "schedule_allocation_latency_seconds_count",
            "schedule_allocation_latency_seconds_sum",
            "runnable_allocations",
            # Allocation runs
            "allocation_runs_in_progress",
            "allocation_runs_total",
            "allocation_run_errors_total",
            "allocation_run_latency_seconds_count",
            "allocation_run_latency_seconds_sum",
            # gRPC channel creation
            "grpc_server_channel_creations_total",
            "grpc_server_channel_creation_retries_total",
            "grpc_server_channel_creation_latency_seconds_count",
            "grpc_server_channel_creation_latency_seconds_sum",
            # Executor state reporting
            "state_report_rpcs_total",
            "state_report_rpc_errors_total",
            "state_report_rpc_latency_seconds_count",
            "state_report_rpc_latency_seconds_sum",
            "state_report_message_size_mb_count",
            "state_report_message_size_mb_sum",
            "state_report_messages_over_size_limit_total",
            "state_report_message_fragmentations_total",
            # Executor state reconciliation
            "state_reconciliations_total",
            "state_reconciliation_errors_total",
            "state_reconciliation_latency_seconds_count",
            "state_reconciliation_latency_seconds_sum",
            "last_desired_state_allocations",
            "last_desired_state_function_executors",
        ]
        metrics: Dict[str, Metric] = fetch_metrics(self)

        for expected_sample_name in expected_sample_names:
            assert_sample_exists(self, metrics, expected_sample_name)

    def test_executor_info_and_state(self):
        metrics: Dict[str, Metric] = fetch_metrics(self)
        info_metric: Metric = get_metric(self, metrics, "executor_info")
        self.assertEqual(len(info_metric.samples), 1)
        info_sample: Sample = info_metric.samples[0]
        self.assertIn("id", info_sample.labels)
        self.assertIn("version", info_sample.labels)
        self.assertIn("cache_path", info_sample.labels)
        self.assertIn("grpc_server_addr", info_sample.labels)
        self.assertIn("config_path", info_sample.labels)
        self.assertIn("hostname", info_sample.labels)
        self.assertEqual(info_sample.value, 1.0)

        state_metric: Metric = get_metric(self, metrics, "executor_state")
        for sample in state_metric.samples:
            if sample.labels["executor_state"] == "starting":
                self.assertEqual(sample.value, 0.0)
            elif sample.labels["executor_state"] == "running":
                self.assertEqual(sample.value, 1.0)
            elif sample.labels["executor_state"] == "shutting_down":
                self.assertEqual(sample.value, 0.0)
            else:
                self.fail(
                    f"Unexpected executor state: {sample.labels['executor_state']}"
                )

    def test_expected_metrics_diff_after_successful_function_run(self):
        # Force unique functions with random versions to ensure cold start because we check cold start metrics too.
        deploy_applications(__file__)

        metrics_before: Dict[str, Metric] = fetch_metrics(self)

        request: Request = run_remote_application(
            successful_function_cold_start_1,
            "ignored",
        )
        self.assertEqual(request.output(), "success")

        metrics_after: Dict[str, Metric] = fetch_metrics(self)

        expected_sample_diffs: List[SampleSpec] = [
            # application downloads
            SampleSpec("application_downloads_total", {}, 1.0),
            SampleSpec("application_download_errors_total", {}, 0.0),
            SampleSpec("application_downloads_from_cache_total", {}, 0.0),
            SampleSpec("application_download_latency_seconds_count", {}, 1.0),
            # allocation preparations
            SampleSpec("allocation_preparations_total", {}, 1.0),
            SampleSpec("allocation_preparation_errors_total", {}, 0.0),
            SampleSpec("allocation_preparation_latency_seconds_count", {}, 1.0),
            SampleSpec("allocations_getting_prepared", {}, 0.0),
            # FE health checker
            SampleSpec("function_executor_failed_health_checks_total", {}, 0.0),
            # Function executor create/destroy.
            SampleSpec("function_executor_creates_total", {}, 1.0),
            SampleSpec("function_executor_create_latency_seconds_count", {}, 1.0),
            SampleSpec("function_executor_create_errors_total", {}, 0.0),
            #
            SampleSpec("function_executor_destroy_errors_total", {}, 0.0),
            #
            SampleSpec(
                "function_executor_create_server_latency_seconds_count", {}, 1.0
            ),
            SampleSpec("function_executor_create_server_errors_total", {}, 0.0),
            #
            SampleSpec("function_executor_destroy_server_errors_total", {}, 0.0),
            #
            SampleSpec(
                "function_executor_establish_channel_latency_seconds_count", {}, 1.0
            ),
            SampleSpec("function_executor_establish_channel_errors_total", {}, 0.0),
            #
            SampleSpec("function_executor_destroy_channel_errors_total", {}, 0.0),
            #
            SampleSpec("function_executor_get_info_rpc_errors_total", {}, 0.0),
            SampleSpec("function_executor_get_info_rpc_latency_seconds_count", {}, 1.0),
            #
            SampleSpec(
                "function_executor_initialize_rpc_latency_seconds_count", {}, 1.0
            ),
            SampleSpec("function_executor_initialize_rpc_errors_total", {}, 0.0),
            #
            SampleSpec(
                "function_executor_create_health_checker_latency_seconds_count", {}, 1.0
            ),
            SampleSpec("function_executor_create_health_checker_errors_total", {}, 0.0),
            #
            SampleSpec(
                "function_executor_destroy_health_checker_errors_total", {}, 0.0
            ),
            # Executor
            SampleSpec("executor_state", {"executor_state": "starting"}, 0.0),
            SampleSpec("executor_state", {"executor_state": "running"}, 0.0),
            SampleSpec("executor_state", {"executor_state": "shutting_down"}, 0.0),
            # allocation lifecycle steps
            SampleSpec("allocations_fetched_total", {}, 1.0),
            SampleSpec(
                "allocations_completed_total",
                {"outcome_code": "all", "failure_reason": "all"},
                1.0,
            ),
            SampleSpec(
                "allocations_completed_total",
                {"outcome_code": "success", "failure_reason": "none"},
                1.0,
            ),
            SampleSpec(
                "allocations_completed_total",
                {"outcome_code": "failure", "failure_reason": "function_error"},
                0.0,
            ),
            SampleSpec(
                "allocations_completed_total",
                {"outcome_code": "failure", "failure_reason": "internal_error"},
                0.0,
            ),
            SampleSpec(
                "allocations_completed_total",
                {
                    "outcome_code": "failure",
                    "failure_reason": "function_executor_terminated",
                },
                0.0,
            ),
            SampleSpec("allocation_completion_latency_seconds_count", {}, 1.0),
            # allocation finalization
            SampleSpec("allocation_finalizations_total", {}, 1.0),
            SampleSpec("allocation_finalization_errors_total", {}, 0.0),
            SampleSpec("allocation_finalization_latency_seconds_count", {}, 1.0),
            # allocation scheduling
            SampleSpec("schedule_allocation_latency_seconds_count", {}, 1.0),
            SampleSpec("runnable_allocations", {}, 0.0),
            # Allocation runs
            SampleSpec("allocation_runs_in_progress", {}, 0.0),
            SampleSpec("allocation_runs_total", {}, 1.0),
            SampleSpec("allocation_run_errors_total", {}, 0.0),
            SampleSpec("allocation_run_latency_seconds_count", {}, 1.0),
            # Server gRPC channel creation
            SampleSpec("grpc_server_channel_creations_total", {}, 0.0),
            SampleSpec("grpc_server_channel_creation_retries_total", {}, 0.0),
            SampleSpec("grpc_server_channel_creation_latency_seconds_count", {}, 0.0),
            # Executor state reporting
            SampleSpec("state_report_rpc_errors_total", {}, 0.0),
            # Executor state reconciliation
            SampleSpec("state_reconciliation_errors_total", {}, 0.0),
        ]
        for expected_diff in expected_sample_diffs:
            sample_before: Sample = get_sample(
                self, metrics_before, expected_diff.name, expected_diff.labels
            )
            sample_after: Sample = get_sample(
                self, metrics_after, expected_diff.name, expected_diff.labels
            )
            actual_value_diff: float = sample_after.value - sample_before.value
            self.assertEqual(
                actual_value_diff,
                expected_diff.value,
                f"Sample {expected_diff.name} with labels {expected_diff.labels} has value diff {actual_value_diff}",
            )

    def test_expected_metrics_after_successful_function_run(self):
        # Force unique functions with random versions to ensure cold start because we check cold start metrics too.
        deploy_applications(__file__)

        request: Request = run_remote_application(
            successful_function_cold_start_2,
            "ignored",
        )
        self.assertEqual(request.output(), "success")

        metrics: Dict[str, Metric] = fetch_metrics(self)
        expected_metrics: List[SampleSpec] = [
            # Running an allocation
            SampleSpec(
                "runnable_allocations_per_function_name",
                {"function_name": "successful_function_cold_start_2"},
                0.0,
            ),
        ]
        for expected_metric in expected_metrics:
            sample: Sample = get_sample(
                self, metrics, expected_metric.name, expected_metric.labels
            )
            self.assertEqual(
                sample.value,
                expected_metric.value,
                f"Sample {expected_metric.name} with labels {expected_metric.labels} has value {sample.value} instead of {expected_metric.value}",
            )


class TestStateHanlers(unittest.TestCase):
    def test_reported_state_handler_success(self):
        response = httpx.get("http://localhost:7000/state/reported")
        self.assertEqual(response.status_code, 200)

    def test_desired_state_handler_success(self):
        response = httpx.get("http://localhost:7000/state/desired")
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
