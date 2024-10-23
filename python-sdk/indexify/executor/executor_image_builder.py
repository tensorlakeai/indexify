import os
import subprocess
from typing import List


def record_image_name(name: str):
    file_path = os.path.expanduser("~/.indexify/image_name")

    if os.path.exists(file_path):
        with open(file_path, "w") as file:
            file.write(name)


def install_dependencies(run_str: str):
    # Throw error to the caller if these subprocesses fail.
    subprocess.run(run_str.split())
