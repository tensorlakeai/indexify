import os
import subprocess

from rich.console import Console
from rich.text import Text
from rich.theme import Theme

from indexify.functions_sdk.image import ImageInformation

custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red",
        "success": "green",
    }
)

console = Console(theme=custom_theme)


def _record_image_name(name: str, version: int):
    dir_path = os.path.expanduser("~/.indexify/")

    file_path = os.path.expanduser("~/.indexify/image_name")
    os.makedirs(dir_path, exist_ok=True)
    with open(file_path, "w") as file:
        file.write(name)

    file_path = os.path.expanduser("~/.indexify/image_version")
    os.makedirs(dir_path, exist_ok=True)
    with open(file_path, "w") as file:
        file.write(str(version))


def _install_dependencies(run_str: str):
    # Throw error to the caller if these subprocesses fail.
    proc = subprocess.run(run_str.split())
    if proc.returncode != 0:
        raise Exception(f"Unable to install dep `{run_str}`")


def executor_image_builder(
    image_info: ImageInformation, name_alias: str, image_version: int
):
    console.print(Text("Attempting Executor Bootstrap.", style="red bold"))

    run_strs = image_info.run_strs
    console.print(Text("Attempting to install dependencies.", style="red bold"))

    for run_str in run_strs:
        console.print(Text(f"Attempting {run_str}", style="red bold"))
        _install_dependencies(run_str)

    console.print(Text("Install dependencies done.", style="red bold"))

    console.print(
        Text(
            f"Recording image name {name_alias} and version {image_version}",
            style="red bold",
        )
    )

    _record_image_name(name_alias, image_version)
