import os


def running_with_gvisor_runtime() -> bool:
    return os.environ.get("DATAPLANE_RUNTIME", "") == "runsc"


def running_on_github_gpu_runner() -> bool:
    return os.environ.get("GITHUB_RUNNER_GPU_TEST", "0") == "1"
