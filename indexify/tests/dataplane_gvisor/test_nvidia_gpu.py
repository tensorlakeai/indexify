import unittest
from typing import Dict, List

from tensorlake.applications import (
    Request,
    application,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications
from testing import running_on_github_gpu_runner

GPU_SPEC = ["T4:1"]
GPU_COUNT = 1


@application()
@function(gpu=GPU_SPEC)
def nvidia_smi_gpu_query(_: str) -> str:
    import subprocess

    result: subprocess.CompletedProcess = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,uuid",
            "--format=csv,noheader",
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    print("nvidia-smi stdout:", result.stdout)
    print("nvidia-smi stderr:", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"nvidia-smi failed with error code {result.returncode}")
    return result.stdout


@application()
@function(gpu=GPU_SPEC)
def pytorch_cuda_is_available(_: str) -> bool:
    import torch

    return torch.cuda.is_available()


@application()
@function(gpu=GPU_SPEC)
def pytorch_cuda_device_count(_: str) -> int:
    import torch

    return torch.cuda.device_count()


@application()
@function(gpu=GPU_SPEC)
def pytorch_compute_tensor(use_gpu: bool) -> Dict[str, List[List[int]]]:
    import torch

    x = torch.tensor([[1, 2, 3], [4, 5, 6]])
    y = torch.tensor([[7, 8, 9], [10, 11, 12]])
    if use_gpu:
        gpu_device = torch.device("cuda:0")
        x = x.to(gpu_device)
        y = y.to(gpu_device)
    return {"tensor": (x + y).tolist()}


@application()
@function()
def pytorch_cuda_device_count_no_gpu(_: str) -> int:
    import torch

    return torch.cuda.device_count()


@unittest.skipUnless(
    running_on_github_gpu_runner(), "GPU only test that depends on host configuration"
)
class TestNvidiaGPU(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_pytorch_cuda_reports_expected_gpu_count(self):
        request: Request = run_remote_application(pytorch_cuda_device_count, "")
        self.assertEqual(request.output(), GPU_COUNT)

    def test_pytorch_cuda_reports_expected_gpu_count_no_gpu(self):
        request: Request = run_remote_application(pytorch_cuda_device_count_no_gpu, "")
        self.assertEqual(request.output(), 0)

    def test_nvidia_smi_reports_expected_gpu_count(self):
        request: Request = run_remote_application(nvidia_smi_gpu_query, "")
        nvidia_smi_gpu_count = len(request.output().splitlines())
        self.assertEqual(nvidia_smi_gpu_count, GPU_COUNT)

    def test_pytorch_cuda_is_available(self):
        request: Request = run_remote_application(pytorch_cuda_is_available, "")
        self.assertTrue(request.output())

    def test_pytorch_cpu_and_gpu_tensor_produces_same_result(self):
        cpu_request: Request = run_remote_application(pytorch_compute_tensor, False)
        gpu_request: Request = run_remote_application(pytorch_compute_tensor, True)
        self.assertEqual(cpu_request.output(), gpu_request.output())


if __name__ == "__main__":
    unittest.main()
