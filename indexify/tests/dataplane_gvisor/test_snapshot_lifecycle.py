"""Snapshot lifecycle E2E test.

Tests the full sandbox snapshot lifecycle:
  1. Create a sandbox
  2. Write a marker file inside it (via daemon file API)
  3. Snapshot the sandbox
  4. Wait for snapshot completion (sandbox is auto-terminated)
  5. Create a new sandbox from the snapshot
  6. Verify the marker file survived the snapshot/restore cycle
  7. Cleanup

Requires:
  - A running indexify-server with DATAPLANE_FUNCTIONS_ENABLED=true
  - A running dataplane with Docker (or Docker+gVisor) driver
  - TENSORLAKE_API_URL env var set (e.g. http://localhost:8900)
"""

import json
import os
import time
import unittest
import urllib.error
import urllib.request


def api_url():
    return os.environ.get("TENSORLAKE_API_URL", "http://localhost:8900")


def api_request(method, path, body=None, headers=None):
    """Make an HTTP request to the server API and return parsed JSON."""
    url = f"{api_url()}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}


def proxy_request(method, proxy_url, sandbox_id, path, body=None):
    """Make an HTTP request through the dataplane proxy to a sandbox daemon.

    Returns parsed JSON for application/json responses, raw string for
    octet-stream/text, or empty string for 204 No Content.
    """
    url = f"{proxy_url}{path}"
    if isinstance(body, str):
        data = body.encode()
    elif isinstance(body, bytes):
        data = body
    elif body is not None:
        data = json.dumps(body).encode()
    else:
        data = None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-Tensorlake-Sandbox-Id", sandbox_id)
    if isinstance(body, dict):
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        if not raw:
            return ""
        content_type = resp.headers.get("Content-Type", "")
        if "application/json" in content_type:
            return json.loads(raw)
        return raw.decode("utf-8", errors="replace")


def wait_for_sandbox_running(namespace, sandbox_id, timeout=60):
    """Poll until the sandbox reaches 'running' status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = api_request("GET", f"/v1/namespaces/{namespace}/sandboxes/{sandbox_id}")
        if info["status"] == "running":
            return info
        if info["status"] == "terminated":
            raise RuntimeError(f"Sandbox {sandbox_id} terminated unexpectedly: {info}")
        time.sleep(1)
    raise TimeoutError(
        f"Sandbox {sandbox_id} did not reach running status within {timeout}s"
    )


def wait_for_snapshot_completed(namespace, snapshot_id, timeout=120):
    """Poll until the snapshot reaches 'completed' status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = api_request("GET", f"/v1/namespaces/{namespace}/snapshots/{snapshot_id}")
        if info["status"] == "completed":
            return info
        if info["status"] == "failed":
            raise RuntimeError(
                f"Snapshot {snapshot_id} failed: {info.get('error', 'unknown')}"
            )
        time.sleep(2)
    raise TimeoutError(f"Snapshot {snapshot_id} did not complete within {timeout}s")


def delete_sandbox(namespace, sandbox_id):
    """Delete (terminate) a sandbox, ignoring 404."""
    try:
        api_request("DELETE", f"/v1/namespaces/{namespace}/sandboxes/{sandbox_id}")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def delete_snapshot(namespace, snapshot_id):
    """Delete a snapshot, ignoring 404."""
    try:
        api_request("DELETE", f"/v1/namespaces/{namespace}/snapshots/{snapshot_id}")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


NAMESPACE = "default"
MARKER_CONTENT = "SNAPSHOT_E2E_MARKER_42"
MARKER_PATH = "/root/snapshot_marker.txt"


class TestSnapshotLifecycle(unittest.TestCase):
    """Test full snapshot lifecycle: create → snapshot → restore → verify."""

    sandbox_ids = []
    snapshot_ids = []

    @classmethod
    def tearDownClass(cls):
        """Best-effort cleanup of all sandboxes and snapshots."""
        for sid in cls.sandbox_ids:
            try:
                delete_sandbox(NAMESPACE, sid)
            except Exception:
                pass
        for sid in cls.snapshot_ids:
            try:
                delete_snapshot(NAMESPACE, sid)
            except Exception:
                pass

    def test_snapshot_create_and_restore(self):
        # ── 1. Create a sandbox ──────────────────────────────────────────
        resp = api_request(
            "POST",
            f"/v1/namespaces/{NAMESPACE}/sandboxes",
            body={
                "resources": {"cpus": 0.1, "memory_mb": 256},
            },
        )
        sandbox_id = resp["sandbox_id"]
        self.sandbox_ids.append(sandbox_id)
        print(f"Created sandbox: {sandbox_id}")

        # ── 2. Wait for it to be running ─────────────────────────────────
        info = wait_for_sandbox_running(NAMESPACE, sandbox_id)
        sandbox_url = info.get("sandbox_url")
        self.assertIsNotNone(sandbox_url, "sandbox_url should be set when running")
        print(f"Sandbox running, proxy URL: {sandbox_url}")

        # ── 3. Write a marker file via the daemon file API ───────────────
        proxy_request(
            "PUT",
            sandbox_url,
            sandbox_id,
            f"/api/v1/files?path={MARKER_PATH}",
            body=MARKER_CONTENT,
        )
        print(f"Wrote marker file: {MARKER_PATH}")

        # Verify the marker file is readable (file API returns raw bytes)
        content = proxy_request(
            "GET",
            sandbox_url,
            sandbox_id,
            f"/api/v1/files?path={MARKER_PATH}",
        )
        self.assertIn(
            MARKER_CONTENT, content, "Marker file content mismatch before snapshot"
        )
        print("Verified marker file before snapshot")

        # ── 4. Snapshot the sandbox ──────────────────────────────────────
        snap_resp = api_request(
            "POST",
            f"/v1/namespaces/{NAMESPACE}/sandboxes/{sandbox_id}/snapshot",
        )
        snapshot_id = snap_resp["snapshot_id"]
        self.snapshot_ids.append(snapshot_id)
        self.assertEqual(snap_resp["status"], "in_progress")
        print(f"Snapshot initiated: {snapshot_id}")

        # ── 5. Wait for snapshot completion ──────────────────────────────
        snap_info = wait_for_snapshot_completed(NAMESPACE, snapshot_id)
        self.assertIsNotNone(
            snap_info.get("size_bytes"), "Snapshot should have size_bytes"
        )
        self.assertGreater(
            snap_info["size_bytes"], 0, "Snapshot should have non-zero size"
        )
        print(f"Snapshot completed: {snap_info.get('size_bytes', 0)} bytes")

        # The original sandbox should be auto-terminated after snapshot.
        orig_info = api_request(
            "GET", f"/v1/namespaces/{NAMESPACE}/sandboxes/{sandbox_id}"
        )
        self.assertEqual(
            orig_info["status"],
            "terminated",
            "Original sandbox should be terminated after snapshot",
        )
        print("Original sandbox terminated after snapshot")

        # ── 6. Create a new sandbox from the snapshot ────────────────────
        restore_resp = api_request(
            "POST",
            f"/v1/namespaces/{NAMESPACE}/sandboxes",
            body={
                "snapshot_id": snapshot_id,
            },
        )
        restored_sandbox_id = restore_resp["sandbox_id"]
        self.sandbox_ids.append(restored_sandbox_id)
        print(f"Created restored sandbox: {restored_sandbox_id}")

        # ── 7. Wait for restored sandbox to be running ───────────────────
        restored_info = wait_for_sandbox_running(NAMESPACE, restored_sandbox_id)
        restored_url = restored_info.get("sandbox_url")
        self.assertIsNotNone(restored_url, "Restored sandbox_url should be set")
        print(f"Restored sandbox running, proxy URL: {restored_url}")

        # ── 8. Verify the marker file survived the snapshot/restore ──────
        restored_content = proxy_request(
            "GET",
            restored_url,
            restored_sandbox_id,
            f"/api/v1/files?path={MARKER_PATH}",
        )
        self.assertIn(
            MARKER_CONTENT,
            restored_content,
            f"Marker file should survive snapshot/restore. Got: {restored_content}",
        )
        print("SUCCESS: Marker file preserved through snapshot/restore cycle!")

        # ── 9. Cleanup ───────────────────────────────────────────────────
        delete_sandbox(NAMESPACE, restored_sandbox_id)
        delete_snapshot(NAMESPACE, snapshot_id)
        print("Cleanup complete")


if __name__ == "__main__":
    unittest.main()
