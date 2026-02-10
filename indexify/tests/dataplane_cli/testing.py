import os
import signal
import shutil
import subprocess
import tempfile
import time
import yaml
import socket
from typing import Dict, Any

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
                "port": 8095, # Default, can be overridden
                "listen_addr": "0.0.0.0"
            },
            "monitoring": {
                "port": find_free_port(),
                "listen_addr": "0.0.0.0"
            },
            "labels": self._labels
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
                print(f"Warning: Failed to clean up temp directory {self._temp_dir}: {e}")

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
        print(f"Waiting Dataplane to start at port {port} (attempts left: {attempts_left})")
        time.sleep(1)
    
    raise Exception(f"Dataplane failed to start at port {port}")
