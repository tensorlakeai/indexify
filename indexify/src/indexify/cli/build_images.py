import importlib
import os
from typing import Any, Generator, Set

import click
import docker
import docker.api.build
from docker.errors import BuildError
from docker.models.images import Image as DockerImage
from tensorlake.applications import Image
from tensorlake.applications.image import (
    ImageInformation,
    dockerfile_content,
    image_infos,
)
from tensorlake.applications.remote.application.ignored_code_paths import (
    ignored_code_paths,
)
from tensorlake.applications.remote.application.loader import load_application


@click.command(
    short_help="Builds images for application defined in <application-source-path> directory or file"
)
@click.argument(
    "application-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=True),
)
def build_images(application_path: str):
    try:
        application_source_dir_or_file_path: str = os.path.abspath(application_path)

        ignored_absolute_paths: Set[str] = ignored_code_paths(
            os.path.dirname(application_source_dir_or_file_path)
        )

        load_application(application_source_dir_or_file_path, ignored_absolute_paths)
    except Exception as e:
        click.secho(
            f"Failed to load the application modules, please check the error message: {e}",
            fg="red",
        )
        raise click.Abort

    docker_client: docker.DockerClient = docker.from_env()
    docker_client.ping()

    indexify_version: str = importlib.metadata.version("indexify")
    for image, image_info in image_infos().items():
        image: Image
        image_info: ImageInformation
        click.echo(f"Building image `{image.name}:{image.tag}`")
        image.run(f"pip install 'indexify=={indexify_version}'")

        try:
            built_image, logs_generator = _build(
                image=image, docker_client=docker_client
            )
            built_image: DockerImage
            _print_build_log(logs_generator)
            click.secho(f"Built image: {built_image.tags[0]}", fg="green")
        except BuildError as e:
            raise click.Abort() from e


def _build(
    image: Image, docker_client: docker.DockerClient
) -> tuple[DockerImage, Generator[str, Any, None]]:
    docker_file_content: str = dockerfile_content(image)
    image_name = image.name if ":" in image.name else f"{image.name}:{image.tag}"

    docker.api.build.process_dockerfile = lambda dockerfile, path: (
        "Dockerfile",
        dockerfile,
    )

    try:
        built_image, logs_generator = docker_client.images.build(
            path=".",
            dockerfile=docker_file_content,
            tag=image_name,
            rm=True,
            # pull=True,  # optional: ensures fresh base images
            # forcerm=True,  # optional: always remove intermediate containers
        )
        return built_image, logs_generator
    except BuildError as e:
        click.secho("Docker build failed:", fg="red")
        _print_build_log(e.build_log or [])
        click.secho(str(e), fg="red")
        raise


def _print_build_log(build_logs):
    for log_entry in build_logs:
        if isinstance(log_entry, dict):
            if "stream" in log_entry:
                click.echo(log_entry["stream"].rstrip("\n"))
            elif "status" in log_entry:
                if "id" in log_entry:
                    click.echo(f"{log_entry['status']}: {log_entry['id']}")
                else:
                    click.echo(log_entry["status"])
            if "errorDetail" in log_entry:
                # This is the most useful bit when a RUN command fails
                msg = log_entry["errorDetail"].get("message") or log_entry.get("error")
                if msg:
                    click.secho(msg.rstrip("\n"), fg="red")
        elif isinstance(log_entry, str):
            click.echo(log_entry.rstrip("\n"))
