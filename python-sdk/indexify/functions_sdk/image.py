import hashlib
import importlib
import sys
from typing import List, Optional

from pydantic import BaseModel


# Pydantic object for API
class ImageInformation(BaseModel):
    image_name: str
    tag: str
    base_image: str
    run_strs: List[str]
    image_url: Optional[str] = ""
    sdk_version: str


class Image:
    def __init__(self):
        self._image_name = None
        self._tag = "latest"
        self._base_image = BASE_IMAGE_NAME
        self._python_version = LOCAL_PYTHON_VERSION
        self._run_strs = []
        self._sdk_version = importlib.metadata.version("indexify")

    def name(self, image_name):
        self._image_name = image_name
        return self

    def tag(self, tag):
        self._tag = tag
        return self

    def base_image(self, base_image):
        self._base_image = base_image
        return self

    def run(self, run_str):
        self._run_strs.append(run_str)
        return self

    def to_image_information(self):
        return ImageInformation(
            image_name=self._image_name,
            tag=self._tag,
            base_image=self._base_image,
            run_strs=self._run_strs,
            sdk_version=self._sdk_version,
        )

    def hash(self) -> str:
        hash = hashlib.sha256(
            self._image_name.encode()
        )  # Make a hash of the image name
        hash.update(self._base_image.encode())
        hash.update("".join(self._run_strs).encode())
        hash.update(self._sdk_version.encode())

        return hash.hexdigest()


LOCAL_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
BASE_IMAGE_NAME = f"python:{LOCAL_PYTHON_VERSION}-slim-bookworm"


def GetDefaultPythonImage(python_version: str):
    return (
        Image()
        .name("tensorlake/indexify-executor-default")
        .base_image(f"python:{python_version}-slim-bookworm")
        .tag(python_version)
    )


DEFAULT_IMAGE = GetDefaultPythonImage(LOCAL_PYTHON_VERSION)
