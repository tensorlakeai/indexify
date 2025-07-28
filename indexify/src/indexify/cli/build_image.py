import importlib
from typing import Any, Generator, Tuple

import click
import docker
import docker.api.build
import docker.models
import docker.models.images
from tensorlake.functions_sdk.image import Image
from tensorlake.functions_sdk.workflow_module import (
    WorkflowModuleInfo,
    load_workflow_module_info,
)


@click.command(
    short_help="Build images for graphs/workflows defined in the workflow file"
)
# Path to the file where the graphs/workflows are defined as global variables
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
        if image_names is not None and image.image_name not in image_names:
            click.echo(
                f"Skipping image `{image.image_name}` as it is not in the provided image names."
            )
            continue

        click.echo(f"Building image `{image.image_name}`")

        image.run(f"pip install 'indexify=={indexify_version}'")
        built_image, logs_generator = image.build()
        built_image: docker.models.images.Image
        for output in logs_generator:
            click.secho(output)

        click.secho(f"built image: {built_image.tags[0]}", fg="green")


def build(
    image: Image, docker_client: docker.DockerClient
) -> Tuple[docker.models.images.Image, Generator[str, Any, None]]:
    docker_file = image.dockerfile()
    image_name = f"{image.image_name}:{image.image_tag}"

    docker.api.build.process_dockerfile = lambda dockerfile, path: (
        "Dockerfile",
        dockerfile,
    )

    return docker_client.images.build(
        path=".",
        dockerfile=docker_file,
        tag=image_name,
        rm=True,
    )
