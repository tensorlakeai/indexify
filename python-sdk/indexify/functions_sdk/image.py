import hashlib
import importlib
import sys
import os
import enum
from typing import List, Optional
import pathlib
import logging

from pydantic import BaseModel

import docker


import docker.api.build

docker.api.build.process_dockerfile = lambda dockerfile, path: (
        "Dockerfile",
        dockerfile,
        )


# Pydantic object for API
class ImageInformation(BaseModel):
    image_name: str
    image_hash: str
    image_url: Optional[str] = ""
    sdk_version: str

# Version of the builder used by the builder service

class BuildOpType(enum.Enum):
    RUN = "RUN"
    COPY = "COPY"
    # ADD = "ADD"
    # FROM = "FROM"
    # WORKDIR = "WORKDIR"

HASH_BUFF_SIZE=1024**2

class BuildOp(BaseModel):
    op_type:BuildOpType
    args:List[str]
    
    def hash(self, hash):
        match self.op_type:
            case BuildOpType.RUN:
                hash.update("RUN".encode())
                for a in self.args: hash.update(a.encode())

            case BuildOpType.COPY:
                hash.update("COPY".encode())
                for root, dirs, files in os.walk(self.args[0]):
                    for file in files:
                        filename = pathlib.Path(root, file)
                        with open(filename, "rb") as fp:
                            data = fp.read(HASH_BUFF_SIZE)
                            while data:
                                hash.update(data)
                                data = fp.read(HASH_BUFF_SIZE)
            
            case _:
                raise ValueError(f"Unsupported build op type {self.op_type}")
    
    def render(self):
        match self.op_type:
            case BuildOpType.RUN:
                return f"RUN {"".join(self.args)}"
            case BuildOpType.COPY:
                return f"COPY {self.args[0]} {self.args[1]}"
            case _:
                raise ValueError(f"Unsupported build op type {self.op_type}")


class BuilderImage(BaseModel): 
    namespace: str
    sdk_version:str
    name:str
    build_ops: List[BuildOp]
    hash:str

class Image:
    def __init__(self):
        self._image_name = None
        self._tag = "latest"
        self._base_image = BASE_IMAGE_NAME
        self._python_version = LOCAL_PYTHON_VERSION
        self._build_ops = [] # List of ImageOperation
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
        self._build_ops.append(BuildOp(op_type=BuildOpType.RUN, args=[run_str]))
        return self

    def copy(self, source:str, dest:str):
        self._build_ops.append(BuildOp(op_type=BuildOpType.COPY, args=[source, dest]))
        return self
            
    def to_image_information(self):
        return ImageInformation(
            image_name=self._image_name,
            sdk_version=self._sdk_version,
            image_hash=self.hash()
        )

    def to_builder_image(self, namespace="default"):
        return BuilderImage(    
            namespace=namespace,
            name = self._image_name,
            sdk_version=self._sdk_version,
            hash=self.hash(),
            build_ops=self._build_ops,
        )
    
    def build(self, python_sdk_path:Optional[str]=None, docker_client=None):
        if docker_client is None:
            docker_client = docker.from_env()
            docker_client.ping()

        docker_contents = [
            f"FROM {self._base_image}",
            "RUN mkdir -p ~/.indexify",
            "RUN touch ~/.indexify/image_name",
            f"RUN echo {self._image_name} > ~/.indexify/image_name",
            f"RUN echo {self.hash()} > ~/.indexify/image_hash",
            "WORKDIR /app",
        ]        

        for build_op in self._build_ops:
            docker_contents.append(build_op.render())

        if python_sdk_path is not None:
            logging.info(
                f"Building image {self._image_name} with local version of the SDK"
            )
            if not os.path.exists(python_sdk_path):
                print(f"error: {python_sdk_path} does not exist")
                os.exit(1)
            docker_contents.append(f"COPY {python_sdk_path} /app/python-sdk")
            docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
        else:
            docker_contents.append(f"RUN pip install indexify=={self._sdk_version}")

        docker_file = "\n".join(docker_contents)
        print(docker_file)

        image_name = f"{self._image_name}:{self._tag}"

        return docker_client.images.build(
            path=".",
            dockerfile=docker_file,
            tag=image_name,
            rm=True,
        )

    def hash(self) -> str:
        hash = hashlib.sha256(
            self._image_name.encode()
        )  # Make a hash of the image name
        hash.update(self._base_image.encode())
        for op in self._build_ops:
            op.hash(hash)

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
