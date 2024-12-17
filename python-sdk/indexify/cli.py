from .logging import configure_logging_early, configure_production_logging

configure_logging_early()

import httpx
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


@app.command(help="Build platform images for function names")
def build_platform_image(
    workflow_file_path: Optional[str] = typer.Option(help="Path to the executor cache directory"),
    image_names: Optional[List[str]] = None):
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
                _create_platform_image(obj)





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
        None, help="Name alias for the executor if it's spun up with the base image"
    ),
    image_version: Optional[int] = typer.Option(
        "1", help="Requested Image Version for this executor"
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
        image_version=image_version,
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
        image_version=image_version,
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



IMAGE_SERVICE="http://localhost:8000"

def _create_platform_image(image: Image):
    # TODO: Wire this up to use the proxy with API keys
    response = httpx.post(f"{IMAGE_SERVICE}/images", data=image.to_builder_image().model_dump_json())
    if response.status_code != 200:
        response.raise_for_status()

    image_id = response.json()['id']

    # Get the latest build for this image
    latest_build_response = httpx.get(f"{IMAGE_SERVICE}/images/{image_id}/latest_completed_build")
    if latest_build_response.status_code == 404: # No completed builds, exist. Starting one now.
        logger.info(f"No existing builds found for {image._image_name}")
        create_build_response = httpx.put(f"{IMAGE_SERVICE}/images/{image_id}/build")

        if create_build_response.status_code not in (201, 200):
            response.raise_for_status()

        latest_build = create_build_response.json()
        build_id = latest_build['id']

        # Block and poll while build completes
        logger.info("Waiting for build to complete")
        while latest_build['status'] != 'completed':
            latest_build_response = httpx.get(f"{IMAGE_SERVICE}/build/{build_id}")
            if latest_build_response.status_code != 200:
                latest_build_response.raise_for_status()
            latest_build = latest_build_response.json()
            time.sleep(1)

        logger.info(f"New build is hosted in {latest_build["uri"]}")
    elif latest_build_response.status_code == 200:
        latest_build = latest_build_response.json()
        logger.info(f"Image is already built and hosted on {latest_build['uri']}")
    else:
        response.raise_for_status()

def _create_image(image: Image, python_sdk_path):
    console.print(
        Text("Creating container for ", style="cyan"),
        Text(f"`{image._image_name}`", style="cyan bold"),
    )
    _build_image(image=image, python_sdk_path=python_sdk_path)


def _build_image(image: Image, python_sdk_path: Optional[str] = None):
    built_image, output = image.build(python_sdk_path=python_sdk_path)
    for line in output:
        print(line)
    print(f"built image: {built_image.tags[0]}")
