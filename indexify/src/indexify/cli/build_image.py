import importlib
from typing import Any, Generator, Tuple

import click
import docker
import docker.api.build
from docker.errors import BuildError
from tensorlake.functions_sdk.image import Image
from tensorlake.functions_sdk.workflow_module import (
    WorkflowModuleInfo,
    load_workflow_module_info,
)


@click.command(
    short_help="Build images for graphs/workflows defined in the workflow file"
)
@click.argument(
    "workflow-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "-i",
    "--image-names",
    multiple=True,
    help="Names of images to build. Can be specified multiple times. If not provided, all images will be built.",
)
def build_image(
    workflow_file_path: str,
    image_names: tuple[str, ...] = None,
):
    """
    Build the images associated to an Indexify workflow

    A workflow is defined in a Python file, and the images are built using the local Docker daemon.
    """
    try:
        workflow_module_info: WorkflowModuleInfo = load_workflow_module_info(
            workflow_file_path
        )
    except Exception as e:
        click.secho(
            f"Failed loading workflow file, please check the error message: {e}",
            fg="red",
        )
        raise click.Abort

    docker_client: docker.DockerClient = docker.from_env()
    docker_client.ping()

    indexify_version: str = importlib.metadata.version("indexify")
    for image in workflow_module_info.images.keys():
        image: Image
        if len(image_names) > 0 and image.image_name not in image_names:
            click.echo(
                f"Skipping image `{image.image_name}` as it is not in the provided image names."
            )
            continue

        click.echo(f"Building image `{image.image_name}`")

        image.run(f"pip install 'indexify=={indexify_version}'")
        built_image, logs_generator = _build(image=image, docker_client=docker_client)
        try:
            built_image, logs_generator = _build(
                image=image, docker_client=docker_client
            )
            _print_build_log(logs_generator)
            click.secho(f"built image: {built_image.tags[0]}", fg="green")
        except BuildError as e:
            raise click.Abort() from e

        click.secho(f"built image: {built_image.tags[0]}", fg="green")


def _build(
    image: Image, docker_client: docker.DockerClient
) -> Tuple[docker.models.images.Image, Generator[str, Any, None]]:
    docker_file = image.dockerfile()
    image_name = (
        image.image_name
        if ":" in image.image_name
        else f"{image.image_name}:{image.image_tag}"
    )

    docker.api.build.process_dockerfile = lambda dockerfile, path: (
        "Dockerfile",
        dockerfile,
    )

    try:
        built_image, logs_generator = docker_client.images.build(
            path=".",
            dockerfile=docker_file,
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
