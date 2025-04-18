import subprocess
from enum import Enum
from typing import Any, List

from pydantic import BaseModel
from tensorlake.functions_sdk.resources import GPU_MODEL


# Only NVIDIA GPUs currently supported in Tensorlake SDK are listed here.
class NVIDIA_GPU_MODEL(str, Enum):
    UNKNOWN = "UNKNOWN"
    A100_40GB = GPU_MODEL.A100_40GB
    A100_80GB = GPU_MODEL.A100_80GB
    H100_80GB = GPU_MODEL.H100


class NvidiaGPUInfo(BaseModel):
    index: str
    uuid: str
    product_name: str  # The official product name.
    model: NVIDIA_GPU_MODEL


def nvidia_gpus_are_available() -> bool:
    try:
        result: subprocess.CompletedProcess = subprocess.run(
            ["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        return result.returncode == 0
    except Exception:
        return False


def fetch_nvidia_gpu_infos(logger: Any) -> List[NvidiaGPUInfo]:
    logger = logger.bind(module=__name__)
    logger.info("Fetching GPU information")

    try:
        result: subprocess.CompletedProcess = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,name,uuid", "--format=csv,noheader"],
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("Failed to fetch GPU information", exc_info=e)
        raise

    infos: List[NvidiaGPUInfo] = []
    for line in result.stdout.splitlines():
        # Example:
        # nvidia-smi --query-gpu=index,name,uuid --format=csv,noheader
        # 0, NVIDIA A100-SXM4-80GB, GPU-89fdc1e1-18b2-f499-c12b-82bcb9bfb3fa
        # 1, NVIDIA A100-PCIE-40GB, GPU-e9c9aa65-bff3-405a-ab7c-dc879cc88169
        # 2, NVIDIA H100 80GB HBM3, GPU-8c35f4c9-4dff-c9a2-866f-afb5d82e1dd7
        parts = line.split(",")
        index = parts[0].strip()
        product_name = parts[1].strip()
        uuid = parts[2].strip()

        model = NVIDIA_GPU_MODEL.UNKNOWN
        if product_name.startswith("NVIDIA A100") and product_name.endswith("80GB"):
            model = NVIDIA_GPU_MODEL.A100_80GB
        if product_name.startswith("NVIDIA A100") and product_name.endswith("40GB"):
            model = NVIDIA_GPU_MODEL.A100_40GB
        elif product_name.startswith("NVIDIA H100"):
            model = NVIDIA_GPU_MODEL.H100_80GB

        if model == NVIDIA_GPU_MODEL.UNKNOWN:
            logger.warning("Unknown GPU model detected", nvidia_smi_output=line)

        infos.append(
            NvidiaGPUInfo(
                index=index, uuid=uuid, product_name=product_name, model=model
            )
        )
    return infos
