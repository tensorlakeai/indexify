import subprocess
from dataclasses import dataclass
from enum import Enum
from typing import Any, List


# Only NVIDIA GPUs currently supported in Tensorlake SDK are listed here.
# GPU models coming with multiple memory sizes have a different enum value per memory size.
class NVIDIA_GPU_MODEL(str, Enum):
    UNKNOWN = "UNKNOWN"
    A100_40GB = "A100-40GB"
    A100_80GB = "A100-80GB"
    H100_80GB = "H100-80GB"
    TESLA_T4 = "T4"
    A6000 = "A6000"
    A10 = "A10"


@dataclass
class NvidiaGPUInfo:
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
        # 3, Tesla T4, GPU-2a7fadae-a692-1c44-2c57-6645a0d117e4
        # 4, NVIDIA RTX A6000, GPU-efe4927a-743f-e4cc-28bb-da604f545b6d
        # 5, NVIDIA A10, GPU-12463b8c-40bb-7322-6c7a-ef48bd7bd39b
        parts = line.split(",")
        index = parts[0].strip()
        product_name = parts[1].strip()
        uuid = parts[2].strip()

        model = _product_name_to_model(product_name)
        if model == NVIDIA_GPU_MODEL.UNKNOWN:
            logger.warning(
                "Unknown GPU model was detected, ignoring", nvidia_smi_output=line
            )
        infos.append(
            NvidiaGPUInfo(
                index=index, uuid=uuid, product_name=product_name, model=model
            )
        )

    return infos


def _product_name_to_model(product_name: str) -> NVIDIA_GPU_MODEL:
    if product_name.startswith("NVIDIA A100") and product_name.endswith("80GB"):
        return NVIDIA_GPU_MODEL.A100_80GB
    if product_name.startswith("NVIDIA A100") and product_name.endswith("40GB"):
        return NVIDIA_GPU_MODEL.A100_40GB
    elif product_name.startswith("NVIDIA H100") and "80GB" in product_name:
        return NVIDIA_GPU_MODEL.H100_80GB
    elif product_name.startswith("Tesla T4"):
        return NVIDIA_GPU_MODEL.TESLA_T4
    elif product_name.startswith("NVIDIA RTX A6000"):
        return NVIDIA_GPU_MODEL.A6000
    elif product_name.startswith("NVIDIA A10"):
        return NVIDIA_GPU_MODEL.A10
    else:
        return NVIDIA_GPU_MODEL.UNKNOWN
