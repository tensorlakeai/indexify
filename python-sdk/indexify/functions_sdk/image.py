from typing import List, Optional

from pydantic import BaseModel


def python_version_to_image(python_version):
    if python_version.startswith("3.9"):
        return "python:3.9.20-bookworm"
    elif python_version.startswith("3.10"):
        return "python:3.10.15-bookworm"
    elif python_version.startswith("3.11"):
        return "python:3.11.10-bookworm"
    else:
        raise ValueError(f"unsupported Python version: {python_version}")


# Pydantic object for API
class ImageInformation(BaseModel):
    image_name: str
    tag: str
    base_image: str
    run_strs: List[str]
    indexify_version: Optional[str]


class Image:
    def __init__(self, python="3.10"):
        self._image_name = None
        self._tag = "latest"
        self._base_image = python_version_to_image(python)
        self._python_version = python
        self._run_strs = []
        self._indexify_version = None

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

    def indexify_version(self, indexify_version):
        self.indexify_version = indexify_version

    def to_image_information(self):
        return ImageInformation(
            image_name=self._image_name,
            tag=self._tag,
            base_image=self._base_image,
            run_strs=self._run_strs,
            indexify_version=self._indexify_version,
        )


DEFAULT_IMAGE_3_10 = (
    Image()
    .name("tensorlake/indexify-executor-default")
    .base_image("python:3.10.15-slim-bookworm")
    .tag("3.10")
)

DEFAULT_IMAGE_3_11 = (
    Image()
    .name("tensorlake/indexify-executor-default")
    .base_image("python:3.11.10-slim-bookworm")
    .tag("3.11")
)
