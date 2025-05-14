from tensorlake.utils.logging import (
    configure_development_mode_logging,
    configure_logging_early,
    configure_production_mode_logging,
)

configure_logging_early()

import os
import shutil
import sys
from importlib.metadata import version
from pathlib import Path
from socket import gethostname
from typing import Annotated, Dict, List, Optional

import nanoid
import prometheus_client
import structlog
import typer
from rich.console import Console
from rich.text import Text
from rich.theme import Theme
from tensorlake.functions_sdk.image import Image

from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.blob_store.local_fs_blob_store import LocalFSBLOBStore
from indexify.executor.blob_store.s3_blob_store import S3BLOBStore
from indexify.executor.executor import Executor
from indexify.executor.function_executor.server.subprocess_function_executor_server_factory import (
    SubprocessFunctionExecutorServerFactory,
)
from indexify.executor.host_resources.host_resources import HostResourcesProvider
from indexify.executor.host_resources.nvidia_gpu_allocator import NvidiaGPUAllocator
from indexify.executor.monitoring.health_checker.generic_health_checker import (
    GenericHealthChecker,
)

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
            f"Could not find workflow file to execute at: `{workflow_file_path}`"
        )
    for _, obj in globals_dict.items():
        if type(obj) and isinstance(obj, Image):
            if image_names is None or obj._image_name in image_names:
                _create_image(obj, python_sdk_path)


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    help="Runs Executor that connects to the Indexify server and starts running its tasks",
)
def executor(
    ctx: typer.Context,
    server_addr: str = "localhost:8900",
    grpc_server_addr: str = "localhost:8901",
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Verbose logging")
    ] = False,
    very_verbose: Annotated[
        bool, typer.Option("--very-verbose", "-vv", help="Very verbose logging")
    ] = False,
    function_uris: Annotated[
        List[str],
        typer.Option(
            "--function",
            "-f",
            help="Functions that the executor will run "
            "specified as <namespace>:<workflow>:<function>:<version>"
            "version is optional, not specifying it will make the server send any version"
            "of the function. Any number of --function arguments can be passed.",
        ),
    ] = [],
    config_path: Optional[str] = typer.Option(
        None, help="Path to the TLS configuration file"
    ),
    executor_cache: Optional[str] = typer.Option(
        "~/.indexify/executor_cache", help="Path to the executor cache directory"
    ),
    monitoring_server_host: Annotated[
        str,
        typer.Option(
            "--monitoring-server-host",
            help="IP address or hostname where to run Executor Monitoring server",
        ),
    ] = "localhost",
    monitoring_server_port: Annotated[
        int,
        typer.Option(
            "--monitoring-server-port",
            help="Port where to run Executor Monitoring server",
        ),
    ] = 7000,
    labels: Annotated[
        List[str],
        typer.Option(
            "--label",
            "-l",
            help="Executor key-value label to be sent to the Server. "
            "Specified as <key>=<value>",
        ),
    ] = [],
):
    if verbose or very_verbose:
        configure_development_mode_logging(compact_tracebacks=not very_verbose)
    else:
        configure_production_mode_logging()

    kv_labels: Dict[str, str] = {}
    for label in labels:
        key, value = label.split("=")
        kv_labels[key] = value

    executor_id: str = nanoid.generate()
    executor_version = version("indexify")
    logger = structlog.get_logger(module=__name__, executor_id=executor_id)

    logger.info(
        "starting executor",
        hostname=gethostname(),
        server_addr=server_addr,
        grpc_server_addr=grpc_server_addr,
        config_path=config_path,
        executor_version=executor_version,
        labels=kv_labels,
        executor_cache=executor_cache,
        functions=function_uris,
        verbose=verbose,
        very_verbose=very_verbose,
        monitoring_server_host=monitoring_server_host,
        monitoring_server_port=monitoring_server_port,
    )
    if ctx.args:
        logger.warning(
            "Unknown arguments passed to the executor",
            unknown_args=ctx.args,
        )
    if len(function_uris) == 0:
        logger.warning(
            "No --function arguments were passed. Executor will run all functions. This scenario is only supported for testing purposes.",
        )

    executor_cache = Path(executor_cache).expanduser().absolute()
    if os.path.exists(executor_cache):
        shutil.rmtree(executor_cache)
    Path(executor_cache).mkdir(parents=True, exist_ok=True)

    blob_store: BLOBStore = BLOBStore(
        # Local FS mode is used in tests and in cases when user wants to store data on NFS.
        local=LocalFSBLOBStore(),
        # S3 is initiliazed lazily so it's okay to create it even if the user is not going to use it.
        s3=S3BLOBStore(),
    )

    host_resources_provider: HostResourcesProvider = HostResourcesProvider(
        gpu_allocator=NvidiaGPUAllocator(logger),
        # Assuming a simple setup in OSS where Executor container has a single file system
        # used by all Function Executors and all the container resources are available to all Function Executors.
        function_executors_ephimeral_disks_path="/",
        host_overhead_cpus=0,
        host_overhead_memory_gb=0,
        host_overhead_function_executors_ephimeral_disks_gb=0,
    )

    prometheus_client.Info("cli", "CLI information").info(
        {
            "package": "indexify",
        }
    )

    Executor(
        id=executor_id,
        version=executor_version,
        labels=kv_labels,
        health_checker=GenericHealthChecker(),
        code_path=executor_cache,
        function_uris=function_uris,
        function_executor_server_factory=SubprocessFunctionExecutorServerFactory(
            verbose_logs=verbose or very_verbose
        ),
        server_addr=server_addr,
        grpc_server_addr=grpc_server_addr,
        config_path=config_path,
        monitoring_server_host=monitoring_server_host,
        monitoring_server_port=monitoring_server_port,
        blob_store=blob_store,
        host_resources_provider=host_resources_provider,
    ).run()


def _create_image(image: Image, python_sdk_path):
    console.print(
        Text("Creating image for ", style="cyan"),
        Text(f"`{image._image_name}`", style="cyan bold"),
    )
    _build_image(image=image, python_sdk_path=python_sdk_path)


def _build_image(image: Image, python_sdk_path: Optional[str] = None):
    built_image, generator = image.build(python_sdk_path=python_sdk_path)
    for output in generator:
        print(output)
    print(f"built image: {built_image.tags[0]}")
