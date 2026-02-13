"""
Integration test for container autoscaling and the containers API.

Tests the full lifecycle:
1. Deploy an application with min/max container limits
2. Buffer reconciler creates min_containers on deploy
3. Sending concurrent requests scales up but never beyond max_containers
4. Deleting the application cleans up all containers
"""

import time

from tensorlake.applications import (
    application,
    function,
)

# -- Application definitions (loaded by function-executor as modules) ----------

_MIN_CONTAINERS = 2
_MAX_CONTAINERS = 5


@application()
@function(min_containers=_MIN_CONTAINERS, max_containers=_MAX_CONTAINERS)
def buffer_test_app(x: int) -> int:
    """Sleep to keep the container busy so the scheduler must scale up."""
    time.sleep(15)
    return x * 2


# -- Everything below is test infrastructure and must only run as __main__ -----
# The function-executor imports this file as a module; imports like `requests`
# or `unittest` are not available in that environment.

if __name__ == "__main__":
    import os
    import unittest
    from typing import List

    import requests as http_requests
    from tensorlake.applications import (
        Request,
        run_remote_application,
    )
    from tensorlake.applications.remote.deploy import deploy_applications

    _API_URL = os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")
    _NAMESPACE = "default"

    def _get_containers(application_name: str) -> List[dict]:
        """Call the containers API for the given application."""
        url = (
            f"{_API_URL}/v1/namespaces/{_NAMESPACE}"
            f"/applications/{application_name}/containers"
        )
        resp = http_requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json().get("containers", [])

    def _delete_application(application_name: str) -> None:
        """Delete an application to free its containers and resources."""
        url = (
            f"{_API_URL}/v1/namespaces/{_NAMESPACE}" f"/applications/{application_name}"
        )
        resp = http_requests.delete(url, timeout=10)
        resp.raise_for_status()

    def _active_containers(containers: List[dict]) -> List[dict]:
        """Return containers that are not terminated.

        A container is considered terminated when either the server's
        desired_state or the executor's current_state says so.
        """
        return [
            c
            for c in containers
            if not c.get("desired_state", "").startswith("terminated")
            and not c.get("current_state", "").startswith("terminated")
        ]

    def _wait_for_containers(
        application_name: str,
        expected_count: int,
        timeout: int = 60,
    ) -> List[dict]:
        """Poll until at least expected_count active containers exist."""
        deadline = time.time() + timeout
        last_count = -1

        while time.time() < deadline:
            containers = _get_containers(application_name)
            active = _active_containers(containers)

            if len(active) != last_count:
                print(
                    f"  active containers: {len(active)}/{expected_count} "
                    f"(elapsed {timeout - int(deadline - time.time())}s)"
                )
                last_count = len(active)

            if len(active) >= expected_count:
                return containers

            time.sleep(2)

        raise TimeoutError(
            f"Expected >= {expected_count} active containers for "
            f"{application_name}, got {last_count} after {timeout}s"
        )

    def _wait_for_no_containers(
        application_name: str,
        timeout: int = 60,
    ) -> None:
        """Poll until zero active containers remain for the application."""
        deadline = time.time() + timeout
        last_count = -1

        while time.time() < deadline:
            containers = _get_containers(application_name)
            active = _active_containers(containers)

            if len(active) != last_count:
                print(
                    f"  active containers: {len(active)}/0 "
                    f"(elapsed {timeout - int(deadline - time.time())}s)"
                )
                last_count = len(active)

            if len(active) == 0:
                return

            time.sleep(2)

        raise TimeoutError(
            f"Expected 0 active containers for "
            f"{application_name}, got {last_count} after {timeout}s"
        )

    # -- Tests -----------------------------------------------------------------
    #
    # Tests are named with numeric prefixes to enforce execution order:
    #   1_ deploy and verify min_containers
    #   2_ verify API response format
    #   3_ verify container states
    #   4_ max_containers cap under load
    #   5_ delete application cleans up containers

    class TestContainerAutoscaling(unittest.TestCase):
        """Tests for container autoscaling: min/max limits and cleanup."""

        @classmethod
        def setUpClass(cls):
            deploy_applications(__file__)
            print(
                f"\nWaiting for buffer reconciler to create "
                f"{_MIN_CONTAINERS} containers for '{buffer_test_app.__name__}'..."
            )
            _wait_for_containers(
                buffer_test_app.__name__,
                expected_count=_MIN_CONTAINERS,
            )

        @classmethod
        def tearDownClass(cls):
            """Safety net: delete the application if a test didn't already."""
            try:
                _delete_application(buffer_test_app.__name__)
            except Exception:
                pass

        def test_1_buffer_reconciler_creates_min_containers(self):
            """After deploy, the buffer reconciler creates min_containers
            without any invocations."""
            containers = _get_containers(buffer_test_app.__name__)
            active = _active_containers(containers)

            self.assertGreaterEqual(len(active), _MIN_CONTAINERS)

            for c in active:
                self.assertEqual(c["application_name"], buffer_test_app.__name__)
                self.assertEqual(c["function_name"], buffer_test_app.__name__)
                self.assertNotEqual(c["executor_id"], "")
                self.assertTrue(len(c["id"]) > 0)
                self.assertTrue(len(c["version"]) > 0)

        def test_2_containers_api_response_format(self):
            """The containers endpoint returns all documented fields with
            correct types."""
            containers = _get_containers(buffer_test_app.__name__)
            active = _active_containers(containers)
            self.assertGreater(len(active), 0, "need at least one container")
            container = active[0]

            required_string_fields = [
                "id",
                "application_name",
                "version",
                "function_name",
                "desired_state",
                "current_state",
                "executor_id",
            ]
            for field in required_string_fields:
                self.assertIn(field, container, f"missing field: {field}")
                self.assertIsInstance(container[field], str, f"{field} should be str")

            if container.get("created_at_clock") is not None:
                self.assertIsInstance(container["created_at_clock"], int)

        def test_3_container_states_are_valid(self):
            """Every container's desired_state and current_state is a known
            state string."""
            containers = _get_containers(buffer_test_app.__name__)

            valid_prefixes = ("unknown", "pending", "running", "terminated")

            for c in containers:
                for field in ("desired_state", "current_state"):
                    value = c[field]
                    self.assertTrue(
                        value.startswith(valid_prefixes),
                        f"container {c['id'][:8]}: {field}={value!r} "
                        f"is not a valid state",
                    )

        def test_4_max_containers_not_exceeded(self):
            """Sending more concurrent requests than max_containers should
            scale up to max_containers but never beyond."""
            num_requests = _MAX_CONTAINERS * 3

            print(
                f"\nFiring {num_requests} concurrent requests "
                f"(max_containers={_MAX_CONTAINERS})..."
            )
            pending: List[Request] = []
            for i in range(num_requests):
                req = run_remote_application(buffer_test_app, i)
                pending.append(req)

            # Poll the containers API while requests are running.
            # The function sleeps 15s so we have time to observe scaling.
            max_observed = 0
            for _ in range(10):
                time.sleep(2)
                containers = _get_containers(buffer_test_app.__name__)
                active = _active_containers(containers)
                count = len(active)
                if count != max_observed:
                    print(
                        f"  active containers: {count} "
                        f"(max allowed: {_MAX_CONTAINERS})"
                    )
                max_observed = max(max_observed, count)

                self.assertLessEqual(
                    count,
                    _MAX_CONTAINERS,
                    f"container count {count} exceeded "
                    f"max_containers={_MAX_CONTAINERS}",
                )

            print(f"  peak containers observed: {max_observed}")
            # Collect all results to let requests finish cleanly.
            for req in pending:
                req.output()

        def test_5_delete_application_cleans_up_containers(self):
            """After deleting the application, all its containers are removed."""
            # Verify containers exist before deletion.
            containers = _get_containers(buffer_test_app.__name__)
            active = _active_containers(containers)
            self.assertGreater(len(active), 0, "expected containers before deletion")

            print(f"\nDeleting application '{buffer_test_app.__name__}'...")
            _delete_application(buffer_test_app.__name__)

            print("Waiting for containers to be cleaned up...")
            _wait_for_no_containers(buffer_test_app.__name__)

            containers = _get_containers(buffer_test_app.__name__)
            active = _active_containers(containers)
            self.assertEqual(
                len(active), 0, "containers should be gone after app deletion"
            )

    class TestContainersAPIMisc(unittest.TestCase):
        """Misc containers API tests that don't need a deployed application."""

        def test_empty_application_returns_empty_list(self):
            """Querying containers for a non-existent application returns
            an empty list, not an error."""
            containers = _get_containers("non_existent_app_12345")
            self.assertEqual(containers, [])

    unittest.main()
