import asyncio
import io
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from typing import Annotated, List, Optional

import docker
import nanoid
import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.theme import Theme

from indexify.executor.agent import ExtractorAgent
from indexify.executor.function_worker import FunctionWorker
from indexify.functions_sdk.image import Image

custom_theme = Theme(
    {
        "info": "cyan",
        "warning": "yellow",
        "error": "red",
        "highlight": "magenta",
    }
)

console = Console(theme=custom_theme)

app = typer.Typer(pretty_exceptions_enable=False, no_args_is_help=True)


@app.command(
    help="Run server and executor in dev mode (Not recommended for production.)"
)
def server_dev_mode():
    indexify_server_path = os.path.expanduser("~/.indexify/indexify-server")
    if not os.path.exists(indexify_server_path):
        print("indexify-server not found. Downloading...")
        try:
            download_command = subprocess.check_output(
                ["curl", "-s", "https://getindexify.ai"], universal_newlines=True
            )
            subprocess.run(download_command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"failed to download indexify-server: {e}")
            exit(1)
        try:
            os.makedirs(os.path.dirname(indexify_server_path), exist_ok=True)
            shutil.move("indexify-server", indexify_server_path)
        except Exception as e:
            print(f"failed to move indexify-server to {indexify_server_path}: {e}")
            exit(1)
    print("starting indexify server and executor in dev mode...")
    print("press Ctrl+C to stop the server and executor.")
    print(f"server binary path: {indexify_server_path}")
    commands = [indexify_server_path, "indexify-cli executor"]

    processes = []
    stop_event = threading.Event()

    def handle_output(process):
        for line in iter(process.stdout.readline, ""):
            sys.stdout.write(line)
            sys.stdout.flush()

    def terminate_processes():
        print("Terminating processes...")
        stop_event.set()
        for process in processes:
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"Force killing process {process.pid}")
                    process.kill()

    def signal_handler(sig, frame):
        print("\nCtrl+C pressed. Shutting down...")
        terminate_processes()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    for cmd in commands:
        process = subprocess.Popen(
            cmd.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
        processes.append(process)

        thread = threading.Thread(target=handle_output, args=(process,))
        thread.daemon = True
        thread.start()

    try:
        while True:
            time.sleep(1)
            if all(process.poll() is not None for process in processes):
                print("All processes have finished.")
                break
    except KeyboardInterrupt:
        signal_handler(None, None)
    finally:
        terminate_processes()

    print("Script execution completed.")


@app.command(help="Build image for function names")
def build_image(workflow_file_path: str, func_names: List[str]):
    globals_dict = {}

    try:
        exec(open(workflow_file_path).read(), globals_dict)
    except FileNotFoundError as e:
        raise Exception(
            f"Could not find workflow file to execute at: " f"`{workflow_file_path}`"
        )

    found_funcs = []
    graph = None
    for name, obj in globals_dict.items():
        for func_name in func_names:
            if name == func_name:
                found_funcs.append(name)
                _create_image_for_func(func_name=func_name, func_obj=obj)

    console.print(
        Text(f"Processed functions: ", style="cyan"),
        Text(f"{found_funcs}", style="green"),
    )


@app.command(help="Joins the extractors to the coordinator server")
def executor(
    server_addr: str = "localhost:8900",
    workers: Annotated[
        int, typer.Option(help="number of worker processes for extraction")
    ] = 1,
    config_path: Optional[str] = typer.Option(
        None, help="Path to the TLS configuration file"
    ),
    executor_cache: Optional[str] = typer.Option(
        "~/.indexify/executor_cache", help="Path to the executor cache directory"
    ),
):
    id = nanoid.generate()
    console.print(
        Panel(
            f"Number of workers: {workers}\n"
            f"Config path: {config_path}\n"
            f"Server address: {server_addr}\n"
            f"Executor ID: {id}\n"
            f"Executor cache: {executor_cache}",
            title="Agent Configuration",
            border_style="info",
        )
    )

    function_worker = FunctionWorker(workers=workers)
    from pathlib import Path

    executor_cache = Path(executor_cache).expanduser().absolute()
    if os.path.exists(executor_cache):
        shutil.rmtree(executor_cache)
    Path(executor_cache).mkdir(parents=True, exist_ok=True)

    agent = ExtractorAgent(
        id,
        num_workers=workers,
        function_worker=function_worker,
        server_addr=server_addr,
        config_path=config_path,
        code_path=executor_cache,
    )

    try:
        asyncio.get_event_loop().run_until_complete(agent.run())
    except asyncio.CancelledError as ex:
        console.print(Text(f"Exiting gracefully: {ex}", style="bold yellow"))


def _create_image_for_func(func_name, func_obj):
    console.print(
        Text("Creating container for ", style="cyan"),
        Text(f"`{func_name}`", style="cyan bold"),
    )
    _build_image(image=func_obj.image, func_name=func_name)


def _build_image(image: Image, func_name: str = None):
    try:
        client = docker.from_env()
        client.ping()
    except Exception as e:
        console.print(
            Text("Unable to connect with docker: ", style="red bold"),
            Text(f"{e}", style="red"),
        )
        exit(-1)

    docker_file_str_template = """
FROM {base_image}

WORKDIR /app

"""

    docker_file_str = docker_file_str_template.format(base_image=image._base_image)

    run_strs = ["RUN " + i for i in image._run_strs]

    docker_file_str += "\n".join(run_strs)

    console.print("Creating image using Dockerfile contents:", style="cyan bold")
    console.print(f"{docker_file_str}", style="magenta")

    client = docker.from_env()
    client.images.build(
        fileobj=io.BytesIO(docker_file_str.encode()),
        tag=f"{image._image_name}:{image._tag}",
        rm=True,
    )
