import gzip
import io
import os
import tarfile
from docker import DockerClient, errors as docker_err
import docker
import logging
from pydantic_settings import BaseSettings
from typing import Dict, Any, List
from jinja2 import Template
from .base_extractor import ExtractorWrapper

class ExtractorPackagerConfig(BaseSettings):
    """
    Configuration settings for the extractor packager using Pydantic for environment management.

    Attributes:
        module_name (str): The name of the module where the extractor is defined.
        class_name (str): The name of the extractor class.
        dockerfile_template_path (str): Path to the Dockerfile Jinja2 template. Defaults to "Dockerfile.extractor".
        verbose (bool): Enables verbose logging if set to True. Defaults to False.
        dev (bool): Indicates if the package is being prepared for development. This affects dependency inclusion. Defaults to False.
        gpu (bool): Indicates if the package requires GPU support. Affects how Python dependencies are installed. Defaults to False.
    """
    module_name: str
    class_name: str

    dockerfile_template_path: str = "Dockerfile.extractor"
    verbose: bool = False
    dev: bool = False
    gpu: bool = False

    # Example of using Field to customize env variable names
    # some_other_config: str = Field(default="default_value", env="SOME_OTHER_CONFIG")

    class Config:
        # Tells Pydantic to read from environment variables as well
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = "ignore"


class DockerfileTemplate:
    """
    Manages the rendering of Dockerfile templates using Jinja2.

    Attributes:
        template_path (str): The file path to the Jinja2 template for the Dockerfile.

    Methods:
        configure: Prepares the template with specific configuration parameters.
        render: Renders the Dockerfile template with the provided configuration.
    """
    def __init__(self, template_path: str):
        self.template_path = template_path
        self.template = None
        self._load_template()
        self.configuration_params = {}

    def configure(self, extractor_path: "ExtractorPathWrapper", system_dependencies: List[str], python_dependencies: List[str], additional_pip_flags: str = "", dev: bool = False) -> "DockerfileTemplate":
        self.configuration_params = {
            "extractor_path": extractor_path.format(),
            "module_name": extractor_path.module_name,
            "class_name": extractor_path.class_name,
            "system_dependencies": system_dependencies,
            "python_dependencies": python_dependencies,
            "additional_pip_flags": additional_pip_flags,
            "dev": dev
        }
        return self

    def _load_template(self):
        with open(self.template_path, "r") as f:
            self.template = Template(f.read())

    def render(self, **kwargs) -> str:
        return self.template.render(**self.configuration_params, **kwargs)


class ExtractorPathWrapper:
    """
    Wraps the extractor's module and class names, providing formatting utilities.

    Attributes:
        module_name (str): The name of the module.
        class_name (str): The name of the class.

    Methods:
        format: Returns a formatted string combining module and class names.
        file_name: Returns the Python file name for the module.
    """

    def __init__(self, module_name: str, class_name: str):
        # i.e. "colbertv2"
        self.module_name = module_name
        # i.e. "ColBERTv2Base"
        self.class_name = class_name
    
    def validate(self) -> "ExtractorPathWrapper":
        # nothing to do for now
        return self

    def format(self) -> str:
        return f"{self.module_name}:{self.class_name}"

    def file_name(self) -> str:
        return f"{self.module_name}.py"

class ExtractorPackager:
    """
    Manages the packaging of an extractor into a Docker image, including Dockerfile generation and tarball creation.

    Attributes:
        config (ExtractorPackagerConfig): Configuration for the packager.
        docker_client (DockerClient): Client for interacting with Docker.
        logger (Logger): Logger for the packager.

    Methods:
        package: Orchestrates the packaging process, including Dockerfile generation, tarball creation, and Docker image building.
        _generate_dockerfile: Generates the Dockerfile content based on the configuration.
        _generate_compressed_tarball: Creates a compressed tarball containing the Dockerfile and any additional required files.
        _add_dev_dependencies: Adds development dependencies to the tarball if applicable.
    """

    def __init__(self, config: ExtractorPackagerConfig = None):
        # use default config if not provided
        self.config = config if config else ExtractorPackagerConfig()
        self.docker_client = DockerClient.from_env()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG if self.config.verbose else logging.INFO)
    
        self.extractor_path = ExtractorPathWrapper(config.module_name, config.class_name).validate()
        self.extractor_description = ExtractorWrapper(config.module_name, config.class_name).describe()

    def package(self):
        docker_client = docker.from_env()
        try:
            dockerfile_content = self._generate_dockerfile()
            self.logger.debug(dockerfile_content)
        except Exception as e:
            self.logger.error(f"Failed to generate Dockerfile: {e}")
            raise

        try:
            compressed_tar_stream = io.BytesIO(self._generate_compressed_tarball(dockerfile_content))
        except Exception as e:
            self.logger.error(f"Failed to generate compressed tarball: {e}")
            raise

        try:
            image, build_log = docker_client.images.build(
                tag=self.extractor_description.name,
                fileobj=compressed_tar_stream,
                custom_context=True,
                encoding="gzip",
                rm=True,
                forcerm=True,
                pull=True
            )

            for chunk in build_log:
                if "stream" in chunk:
                    print(chunk["stream"].strip())
                    self.logger.debug(chunk["stream"].strip())
                elif "error" in chunk:
                    print(chunk["error"].strip())
                    self.logger.error(chunk["error"].strip())
            
            self.logger.info(f"Successfully built image {image.tags[0]}")
        
        except docker_err.BuildError as e:
            self.logger.error(f"Failed to build image {self.extractor_description.name}: {e}")
            raise

        except docker_err.APIError as e:
            self.logger.error(f"Docker API Error: {e}")
            raise
                
    def _generate_dockerfile(self) -> str:
        return DockerfileTemplate(self.config.dockerfile_template_path).configure(
            extractor_path=self.extractor_path,
            system_dependencies=" ".join(self.extractor_description.system_dependencies),
            python_dependencies=" ".join(self.extractor_description.python_dependencies),
            additional_pip_flags="" if self.config.gpu else "--extra-index-url https://download.pytorch.org/whl/cpu",
            dev=self.config.dev
        ).render()
    
    def _generate_compressed_tarball(self, dockerfile_content: str) -> bytes:
        """
        Generates a tarball containing the Dockerfile and any additional files needed for the extractor.
        """
        dockerfile_tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=dockerfile_tar_buffer, mode="w|", format=tarfile.GNU_FORMAT) as tar:
            dockerfile_info = tarfile.TarInfo("Dockerfile")
            dockerfile_info.mode = 0o755
            dockerfile_bytes = dockerfile_content.encode("utf-8")
            dockerfile_info.size = len(dockerfile_bytes)
            tar.addfile(dockerfile_info, fileobj=io.BytesIO(dockerfile_bytes))
            if self.config.dev:
                self._add_dev_dependencies(tar)
        
        dockerfile_tar_buffer.seek(0)
        compressed_data_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_data_buffer, mode="wb") as gz:
            gz.write(dockerfile_tar_buffer.getvalue())

        compressed_data_buffer.seek(0)
        return compressed_data_buffer.getvalue()

    def _add_dev_dependencies(self, tar: tarfile.TarFile):
        tar.add("setup.py")
        tar.add("indexify_extractor_sdk", recursive=True)


        
