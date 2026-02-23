import os
import selectors
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, Dict

import yaml


class DataplaneProcessContextManager:
    def __init__(
        self,
        config_overrides: Dict[str, Any],
        keep_std_outputs: bool = True,
        labels: Dict[str, str] = None,
        args: list[str] = None,
        extra_env: Dict[str, str] = None,
    ):
        self._keep_std_outputs = keep_std_outputs
        self._config_overrides = config_overrides
        self._labels = labels or {}
        self._extra_args = args or []
        self._extra_env = extra_env
        self._temp_dir = tempfile.mkdtemp(prefix="dataplane_test_")
        self._process: subprocess.Popen | None = None
        self._config_path = os.path.join(self._temp_dir, "config.yaml")
        self._state_file_path = os.path.join(self._temp_dir, "state.json")

    def _generate_config(self):
        config = {
            "env": "local",
            "server_addr": "http://localhost:8901",
            "driver": {"type": "fork_exec"},
            "state_file": self._state_file_path,
            "http_proxy": {
                "port": 8095,  # Default, can be overridden
                "listen_addr": "0.0.0.0",
            },
            "monitoring": {"port": find_free_port(), "listen_addr": "0.0.0.0"},
            "labels": self._labels,
        }

        # Merge overrides
        self._merge_dicts(config, self._config_overrides)

        with open(self._config_path, "w") as f:
            yaml.dump(config, f)

    def _merge_dicts(self, base: Dict, overrides: Dict):
        for k, v in overrides.items():
            if isinstance(v, dict) and k in base and isinstance(base[k], dict):
                self._merge_dicts(base[k], v)
            else:
                base[k] = v

    def __enter__(self) -> subprocess.Popen:
        self._generate_config()

        dataplane_bin = os.environ.get("DATAPLANE_BIN", "indexify-dataplane")
        args = [dataplane_bin, "--config", self._config_path]
        args.extend(self._extra_args)

        kwargs = {}
        if self._extra_env:
            kwargs["env"] = os.environ.copy()
            kwargs["env"].update(self._extra_env)

        if not self._keep_std_outputs:
            kwargs["stdout"] = subprocess.DEVNULL
            kwargs["stderr"] = subprocess.DEVNULL

        self._process = subprocess.Popen(args, start_new_session=True, **kwargs)
        return self._process

    def __exit__(self, exc_type, exc_value, traceback):
        if self._process:
            # Kill the entire process group (dataplane + forked function
            # executors) so no orphans are left holding file descriptors.
            pgid = os.getpgid(self._process.pid)
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self._process.wait()

        if self._temp_dir:
            try:
                shutil.rmtree(self._temp_dir)
            except Exception as e:
                print(
                    f"Warning: Failed to clean up temp directory {self._temp_dir}: {e}"
                )


def find_free_port() -> int:
    """Return a free TCP port by briefly binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_dataplane_startup(port: int):
    """Wait for Dataplane HTTP proxy port to be open."""
    attempts_left: int = 30
    while attempts_left > 0:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                print(f"Dataplane startup check successful at port {port}")
                return
        except (socket.timeout, ConnectionRefusedError):
            pass
        except Exception:
            if attempts_left == 1:
                raise

        attempts_left -= 1
        print(
            f"Waiting Dataplane to start at port {port} (attempts left: {attempts_left})"
        )
        time.sleep(1)

    raise Exception(f"Dataplane failed to start at port {port}")


class PartitionProxy:
    """TCP proxy that can simulate network partitions.

    Forwards all traffic between a listen port and a target (server gRPC).
    When partitioned, closes all active connections and refuses new ones.
    """

    def __init__(self, target_host: str, target_port: int):
        self.target = (target_host, target_port)
        self.port = find_free_port()
        self._partitioned = threading.Event()  # set = partitioned
        self._server_socket = None
        self._thread = None
        self._connections = []
        self._running = False
        self._lock = threading.Lock()

    def start(self):
        """Start the proxy in a background thread."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.settimeout(0.5)
        self._server_socket.bind(("127.0.0.1", self.port))
        self._server_socket.listen(128)
        self._running = True
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the proxy and close all connections."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass
        self._close_all_connections()

    def partition(self):
        """Simulate a network partition. Closes all active connections
        and refuses new ones until heal() is called."""
        self._partitioned.set()
        self._close_all_connections()

    def heal(self):
        """Restore network connectivity."""
        self._partitioned.clear()

    def _close_all_connections(self):
        """Close all active forwarding connections."""
        with self._lock:
            for client_sock, target_sock in self._connections:
                try:
                    client_sock.close()
                except OSError:
                    pass
                try:
                    target_sock.close()
                except OSError:
                    pass
            self._connections.clear()

    def _accept_loop(self):
        """Main accept loop running in the background thread."""
        while self._running:
            try:
                client_sock, addr = self._server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            if self._partitioned.is_set():
                try:
                    client_sock.close()
                except OSError:
                    pass
                continue

            try:
                target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                target_sock.connect(self.target)
            except OSError:
                try:
                    client_sock.close()
                except OSError:
                    pass
                continue

            with self._lock:
                self._connections.append((client_sock, target_sock))

            fwd_thread = threading.Thread(
                target=self._forward,
                args=(client_sock, target_sock),
                daemon=True,
            )
            fwd_thread.start()

    def _forward(self, client_sock: socket.socket, target_sock: socket.socket):
        """Bidirectionally forward bytes between client and target using selectors."""
        sel = selectors.DefaultSelector()
        try:
            client_sock.setblocking(False)
            target_sock.setblocking(False)
            sel.register(client_sock, selectors.EVENT_READ, data="client")
            sel.register(target_sock, selectors.EVENT_READ, data="target")

            while self._running and not self._partitioned.is_set():
                events = sel.select(timeout=0.5)
                if not events:
                    continue
                for key, mask in events:
                    try:
                        data = key.fileobj.recv(65536)
                    except (OSError, ConnectionError):
                        return
                    if not data:
                        return
                    dest = target_sock if key.data == "client" else client_sock
                    try:
                        dest.sendall(data)
                    except (OSError, ConnectionError):
                        return
        finally:
            sel.close()
            try:
                client_sock.close()
            except OSError:
                pass
            try:
                target_sock.close()
            except OSError:
                pass
            with self._lock:
                try:
                    self._connections.remove((client_sock, target_sock))
                except ValueError:
                    pass
