from .logging import configure_logging_early, configure_production_logging

configure_logging_early()


import asyncio
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from importlib.metadata import version
from typing import Annotated, List, Optional

import nanoid
import structlog
import typer
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.theme import Theme

from indexify.executor.agent import ExtractorAgent
from indexify.function_executor.function_executor_service import (
    FunctionExecutorService,
)
from indexify.function_executor.server import Server as FunctionExecutorServer
from indexify.functions_sdk.image import (
    LOCAL_PYTHON_VERSION,
    GetDefaultPythonImage,
    Image,
)
from indexify.http_client import IndexifyClient

logger = structlog.get_logger(module=__name__)

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
config_path_option: Optional[str] = typer.Option(
    None, help="Path to the TLS configuration file"
)


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
def build_image(
    workflow_file_path: str,
    image_names: Optional[List[str]] = None,
    python_sdk_path: Optional[str] = None,
):
    globals_dict = {}

    # Add the folder in the workflow file path to the current Python path
    folder_path = os.path.dirname(workflow_file_path)
    if folder_path not in sys.path:
        sys.path.append(folder_path)

    try:
        exec(open(workflow_file_path).read(), globals_dict)
    except FileNotFoundError as e:
        raise Exception(
            f"Could not find workflow file to execute at: " f"`{workflow_file_path}`"
        )
    for _, obj in globals_dict.items():
        if type(obj) and isinstance(obj, Image):
            if image_names is None or obj._image_name in image_names:
                _create_image(obj, python_sdk_path)


@app.command(help="Build default image for indexify")
def build_default_image(
    python_version: Optional[str] = typer.Option(
        f"{sys.version_info.major}.{sys.version_info.minor}",
        help="Python version to use in the base image",
    )
):
    image = GetDefaultPythonImage(python_version)

    _build_image(image=image)

    console.print(
        Text(f"Built default indexify image with hash {image.hash()}\n", style="cyan"),
        Text(
            f"Don't forget to update your executors to run this image!", style="yellow"
        ),
    )


@app.command(help="Joins the extractors to the coordinator server")
def executor(
    server_addr: str = "localhost:8900",
    dev: Annotated[
        bool, typer.Option("--dev", "-d", help="Run the executor in development mode")
    ] = False,
    config_path: Optional[str] = config_path_option,
    executor_cache: Optional[str] = typer.Option(
        "~/.indexify/executor_cache", help="Path to the executor cache directory"
    ),
    name_alias: Optional[str] = typer.Option(
        None, help="Image name override for the executor"
    ),
    image_hash: Optional[str] = typer.Option(
        None, help="Image hash override for the executor"
    ),
):
    if not dev:
        configure_production_logging()

    id = nanoid.generate()
    executor_version = version("indexify")
    logger.info(
        "executor started",
        server_addr=server_addr,
        config_path=config_path,
        executor_id=id,
        executor_version=executor_version,
        executor_cache=executor_cache,
        name_alias=name_alias,
        image_hash=image_hash,
        dev_mode=dev,
    )

    from pathlib import Path

    executor_cache = Path(executor_cache).expanduser().absolute()
    if os.path.exists(executor_cache):
        shutil.rmtree(executor_cache)
    Path(executor_cache).mkdir(parents=True, exist_ok=True)

    agent = ExtractorAgent(
        id,
        server_addr=server_addr,
        config_path=config_path,
        code_path=executor_cache,
        name_alias=name_alias,
        image_hash=image_hash,
        development_mode=dev,
    )

    try:
        asyncio.get_event_loop().run_until_complete(agent.run())
    except asyncio.CancelledError:
        logger.info("graceful shutdown")


@app.command(help="Runs a Function Executor server")
def function_executor(
    function_executor_server_address: str = typer.Option(
        help="Function Executor server address"
    ),
    indexify_server_address: str = typer.Option(help="Indexify server address"),
    dev: Annotated[
        bool, typer.Option("--dev", "-d", help="Run the executor in development mode")
    ] = False,
    config_path: Optional[str] = config_path_option,
):
    if not dev:
        configure_production_logging()

    logger.info(
        "starting function executor server",
        function_executor_server_address=function_executor_server_address,
        indexify_server_address=indexify_server_address,
        config_path=config_path,
    )

    FunctionExecutorServer(
        server_address=function_executor_server_address,
        service=FunctionExecutorService(
            indexify_server_address=indexify_server_address, config_path=config_path
        ),
    ).run()


def _create_image(image: Image, python_sdk_path):
    console.print(
        Text("Creating container for ", style="cyan"),
        Text(f"`{image._image_name}`", style="cyan bold"),
    )
    _build_image(image=image, python_sdk_path=python_sdk_path)


def _build_image(image: Image, python_sdk_path: Optional[str] = None):

    try:
        import docker

        client = docker.from_env()
        client.ping()
    except Exception as e:
        console.print(
            Text("Unable to connect with docker: ", style="red bold"),
            Text(f"{e}", style="red"),
        )
        exit(-1)

    docker_contents = [
        f"FROM {image._base_image}",
        "RUN mkdir -p ~/.indexify",
        "RUN touch ~/.indexify/image_name",
        f"RUN echo {image._image_name} > ~/.indexify/image_name",
        f"RUN echo {image.hash()} > ~/.indexify/image_hash",
        "WORKDIR /app",
    ]

    docker_contents.extend(["RUN " + i for i in image._run_strs])

    if python_sdk_path is not None:
        logging.info(
            f"Building image {image._image_name} with local version of the SDK"
        )
        if not os.path.exists(python_sdk_path):
            print(f"error: {python_sdk_path} does not exist")
            os.exit(1)
        docker_contents.append(f"COPY {python_sdk_path} /app/python-sdk")
        docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
    else:
        docker_contents.append(f"RUN pip install indexify=={image._sdk_version}")

    docker_file = "\n".join(docker_contents)

    import docker.api.build

    docker.api.build.process_dockerfile = lambda dockerfile, path: (
        "Dockerfile",
        dockerfile,
    )

    console.print("Creating image using Dockerfile contents:", style="cyan bold")
    print(f"{docker_file}")

    client = docker.from_env()
    image_name = f"{image._image_name}:{image._tag}"
    (_image, generator) = client.images.build(
        path=".",
        dockerfile=docker_file,
        tag=image_name,
        rm=True,
    )
    for result in generator:
        print(result)

    print(f"built image: {image_name}")
