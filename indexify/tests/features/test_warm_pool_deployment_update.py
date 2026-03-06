import json
import os
import time
import unittest
import urllib.error
import urllib.request
import uuid
from typing import List, Set

from tensorlake.applications import (
    Function,
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

_API_URL = os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")
_NAMESPACE = "default"


@application()
@function(
    timeout=120,
    min_containers=1,
    warm_containers=1,
    max_containers=2,
)
def warm_pool_rollout_app(x: int) -> int:
    # Keep requests active long enough to observe multiple containers.
    time.sleep(4)
    return x * 10


def _set_random_version(func: Function) -> str:
    version = str(uuid.uuid4())
    func._application_config.version = version
    return version


def _get_containers(application_name: str) -> List[dict]:
    url = (
        f"{_API_URL}/v1/namespaces/{_NAMESPACE}"
        f"/applications/{application_name}/containers"
    )
    resp = urllib.request.urlopen(url, timeout=10)
    return json.loads(resp.read()).get("containers", [])


def _delete_application(application_name: str) -> None:
    url = f"{_API_URL}/v1/namespaces/{_NAMESPACE}/applications/{application_name}"
    req = urllib.request.Request(url, method="DELETE")
    urllib.request.urlopen(req, timeout=10)


def _active_containers(containers: List[dict]) -> List[dict]:
    return [
        c
        for c in containers
        if not c.get("desired_state", "").startswith("terminated")
        and not c.get("current_state", "").startswith("terminated")
    ]


def _wait_for_active_container_count(
    application_name: str,
    min_count: int,
    timeout: int = 90,
) -> List[dict]:
    deadline = time.time() + timeout
    last_count = -1

    while time.time() < deadline:
        active = _active_containers(_get_containers(application_name))
        if len(active) != last_count:
            print(f"  active containers: {len(active)} (need >= {min_count})")
            last_count = len(active)
        if len(active) >= min_count:
            return active
        time.sleep(2)

    raise TimeoutError(
        f"Expected at least {min_count} active containers, got {last_count}"
    )


def _wait_for_active_versions(
    application_name: str,
    expected_versions: Set[str],
    min_count: int = 1,
    timeout: int = 120,
) -> List[dict]:
    deadline = time.time() + timeout
    last_versions: Set[str] = set()
    last_count = -1

    while time.time() < deadline:
        active = _active_containers(_get_containers(application_name))
        versions = {c["version"] for c in active}

        if len(active) != last_count or versions != last_versions:
            print(f"  active={len(active)} versions={sorted(versions)}")
            last_count = len(active)
            last_versions = versions

        if len(active) >= min_count and versions == expected_versions:
            return active

        time.sleep(2)

    raise TimeoutError(
        "Timed out waiting for active containers to match expected versions. "
        f"expected={sorted(expected_versions)} got={sorted(last_versions)} count={last_count}"
    )


class TestWarmPoolDeploymentUpdate(unittest.TestCase):
    def tearDown(self):
        try:
            _delete_application(warm_pool_rollout_app.__name__)
        except urllib.error.HTTPError:
            pass
        except urllib.error.URLError:
            pass

    def test_warm_pool_containers_update_to_latest_version_after_deploy(self):
        app_name = warm_pool_rollout_app.__name__

        version_1 = _set_random_version(warm_pool_rollout_app)
        deploy_applications(__file__)
        _wait_for_active_versions(app_name, {version_1}, min_count=1)

        request_1: Request = run_remote_application(warm_pool_rollout_app, 1)
        request_2: Request = run_remote_application(warm_pool_rollout_app, 2)
        _wait_for_active_container_count(app_name, min_count=1, timeout=60)
        self.assertEqual(request_1.output(), 10)
        self.assertEqual(request_2.output(), 20)

        version_2 = _set_random_version(warm_pool_rollout_app)
        self.assertNotEqual(version_1, version_2)
        deploy_applications(__file__)

        active_containers = _wait_for_active_versions(app_name, {version_2}, min_count=1)
        active_versions = {c["version"] for c in active_containers}
        self.assertEqual(active_versions, {version_2})
        self.assertNotIn(version_1, active_versions)


if __name__ == "__main__":
    unittest.main()
