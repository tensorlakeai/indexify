import os
import subprocess
from typing import List

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


def _record_image_name(name: str):
    file_path = os.path.expanduser("~/.indexify/image_name")

    if os.path.exists(file_path):
        with open(file_path, "w") as file:
            file.write(name)


def _install_dependencies(run_str: str):
    # Throw error to the caller if these subprocesses fail.
    subprocess.run(run_str.split())


def executor_image_builder(image_info: ImageInformation, name_alias: str):
    console.print(Text("Attempting Executor Bootstrap.", style="red bold"))

    run_strs = image_info.run_strs
    console.print(Text("Attempting to install dependencies", style="red bold"))

    for run_str in run_strs:
        console.print(Text(f"Attempting {run_str}", style="red bold"))
        _install_dependencies(run_str)

    console.print(Text(f"Recording image name {name_alias}", style="red bold"))
    _record_image_name(name_alias)
