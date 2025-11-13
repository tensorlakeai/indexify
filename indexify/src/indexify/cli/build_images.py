import importlib
import os
import traceback
from typing import Any, Generator, Optional

import click
import docker
import docker.api.build
from docker.errors import BuildError
from docker.models.images import Image as DockerImage
from tensorlake.applications import Image
from tensorlake.applications.remote.code.loader import (
    load_code,
)  # pylint: disable=no-name-in-module, import-error
from tensorlake.applications.image import (
    dockerfile_content,
    image_infos,
)


@click.command(
    short_help="Builds images for applications defined in <application-file-path> .py file"
)
@click.argument(
    "application-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "--repository",
    "-r",
    type=str,
    required=False,
    help="The remote repository to push the built images to (e.g. ghcr.io/org, 123456789012.dkr.ecr.region.amazonaws.com)",
)
@click.option(
    "--tag",
    "-t",
    type=str,
    required=False,
    help="Tag to use for the built images (overrides the tag defined in the image)",
)
@click.option(
    "--image-name",
    "-i",
    type=str,
    required=False,
    help="Filter to build only the image with the specified name",
)
@click.option(
    "--push",
    is_flag=True,
    help="Push the built images to the remote registry",
)
def build_images(
    application_file_path: str,
    repository: Optional[str],
    tag: Optional[str],
    image_name: Optional[str],
    push: bool,
):
    try:
        application_file_path = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except Exception as e:  # pylint: disable=broad-exception-caught
        click.secho(
            f"Failed to load the code directory modules, please check the error message: {e}",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    docker_client: docker.DockerClient = docker.from_env()
    docker_client.ping()

    indexify_version: str = importlib.metadata.version("indexify")

    for image, _ in image_infos().items():
        image: Image
        if image_name and image.name != image_name:
            continue

        effective_tag = tag or image.tag

        click.echo(f"Building image `{image.name}:{effective_tag}`")
        image.run(f"pip install 'indexify=={indexify_version}'")

        image_name = (
            image.name if ":" in image.name else f"{image.name}:{effective_tag}"
        )

        try:
            built_image, logs_generator = _build(
                image=image,
                docker_client=docker_client,
                image_name=image_name,
            )
            built_image: DockerImage
            _print_build_log(logs_generator)
            click.secho(f"Built image: {image_name}", fg="green")

            if push:
                target_repo = repository or image.name
                target_tag = effective_tag
                target_image_name = f"{target_repo}:{target_tag}"

                click.echo(
                    f"Tagging image `{image_name}` as `{target_image_name}` before push"
                )
                built_image.tag(target_repo, target_tag)

                click.echo(f"Pushing image `{target_image_name}`")
                push_logs = docker_client.images.push(
                    repository=target_repo,
                    tag=target_tag,
                    stream=True,
                    decode=True,
                )
                _print_build_log(push_logs)
                click.secho(
                    f"Pushed image: {target_image_name}",
                    fg="green",
                )

        except BuildError as e:
            traceback.print_exception(e)
            raise click.Abort


def _build(
    image: Image,
    docker_client: docker.DockerClient,
    image_name: str,
) -> tuple[DockerImage, Generator[str, Any, None]]:
    docker_file_content: str = dockerfile_content(image)

    # Monkey-patch: allow passing Dockerfile content directly
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
    if isinstance(build_logs, str):
        click.echo(build_logs)
        return

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
