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
from typing import Annotated, Dict, List, Optional, Tuple

import nanoid
import prometheus_client
import structlog
import typer
from rich.console import Console
from rich.text import Text
from rich.theme import Theme
from tensorlake.functions_sdk.image import Image

from indexify.executor.api_objects import FunctionURI
from indexify.executor.blob_store.blob_store import BLOBStore
from indexify.executor.blob_store.local_fs_blob_store import LocalFSBLOBStore
from indexify.executor.blob_store.s3_blob_store import S3BLOBStore
from indexify.executor.executor import Executor
from indexify.executor.executor_flavor import ExecutorFlavor
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
            f"Could not find workflow file to execute at: " f"`{workflow_file_path}`"
        )
    for _, obj in globals_dict.items():
        if type(obj) and isinstance(obj, Image):
            if image_names is None or obj._image_name in image_names:
                _create_image(obj, python_sdk_path)


@app.command(
    help="Runs Executor that connects to the Indexify server and starts running its tasks"
)
def executor(
    server_addr: str = "localhost:8900",
    grpc_server_addr: str = "localhost:8901",
    dev: Annotated[
        bool, typer.Option("--dev", "-d", help="Run the executor in development mode")
    ] = False,
    function_uris: Annotated[
        Optional[List[str]],
        typer.Option(
            "--function",
            "-f",
            help="Function that the executor will run "
            "specified as <namespace>:<workflow>:<function>:<version>"
            "version is optional, not specifying it will make the server send any version"
            "of the function",
        ),
    ] = None,
    config_path: Optional[str] = typer.Option(
        None, help="Path to the TLS configuration file"
    ),
    executor_cache: Optional[str] = typer.Option(
        "~/.indexify/executor_cache", help="Path to the executor cache directory"
    ),
    # Registred ports range ends at 49151.
    ports: Tuple[int, int] = typer.Option(
        (50000, 51000),
        help="Range of localhost TCP ports to be used by Function Executors",
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
    enable_grpc_state_reconciler: Annotated[
        bool,
        typer.Option(
            "--enable-grpc-state-reconciler",
            help=(
                "(exprimental) Enable gRPC state reconciler that will reconcile the state of the Function Executors and Task Allocations\n"
                "with the desired state provided by Server. Required --grpc-server-addr to be set."
            ),
        ),
    ] = False,
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
    if dev:
        configure_development_mode_logging()
    else:
        configure_production_mode_logging()
        if function_uris is None:
            raise typer.BadParameter(
                "At least one function must be specified when not running in development mode"
            )

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
        ports=ports,
        functions=function_uris,
        dev_mode=dev,
        monitoring_server_host=monitoring_server_host,
        monitoring_server_port=monitoring_server_port,
        enable_grpc_state_reconciler=enable_grpc_state_reconciler,
    )

    executor_cache = Path(executor_cache).expanduser().absolute()
    if os.path.exists(executor_cache):
        shutil.rmtree(executor_cache)
    Path(executor_cache).mkdir(parents=True, exist_ok=True)

    start_port: int = ports[0]
    end_port: int = ports[1]
    if start_port >= end_port:
        console.print(
            Text(
                f"start port {start_port} should be less than {end_port}", style="red"
            ),
        )
        exit(1)

    # Enable all available blob stores in OSS because we don't know which one is going to be used.
    blob_store: BLOBStore = BLOBStore(
        # Local FS mode is used in tests and in cases when user wants to store data on NFS.
        local=LocalFSBLOBStore(),
        # S3 is initiliazed lazily so it's okay to create it even if the user is not going to use it.
        s3=S3BLOBStore(),
    )

    prometheus_client.Info("cli", "CLI information").info(
        {
            "package": "indexify",
        }
    )

    Executor(
        id=executor_id,
        development_mode=dev,
        flavor=ExecutorFlavor.OSS,
        version=executor_version,
        labels=kv_labels,
        health_checker=GenericHealthChecker(),
        code_path=executor_cache,
        function_allowlist=_parse_function_uris(function_uris),
        function_executor_server_factory=SubprocessFunctionExecutorServerFactory(
            development_mode=dev,
            server_ports=range(ports[0], ports[1]),
        ),
        server_addr=server_addr,
        grpc_server_addr=grpc_server_addr,
        config_path=config_path,
        monitoring_server_host=monitoring_server_host,
        monitoring_server_port=monitoring_server_port,
        enable_grpc_state_reconciler=enable_grpc_state_reconciler,
        blob_store=blob_store,
        host_resources_provider=HostResourcesProvider(NvidiaGPUAllocator(logger)),
    ).run()


def _parse_function_uris(uri_strs: Optional[List[str]]) -> Optional[List[FunctionURI]]:
    if uri_strs is None:
        return None

    uris: List[FunctionURI] = []
    for uri_str in uri_strs:
        tokens = uri_str.split(":")
        # FIXME bring this back when we have a dynamic scheduler
        # if len(tokens) != 4:
        if len(tokens) < 3 and len(tokens) > 4:
            raise typer.BadParameter(
                "Function should be specified as <namespace>:<workflow>:<function>:<version> or"
                "<namespace>:<workflow>:<function>"
            )
        try:
            version = tokens[3]
        except IndexError:
            version = None
        uris.append(
            FunctionURI(
                namespace=tokens[0],
                compute_graph=tokens[1],
                compute_fn=tokens[2],
                version=version,
            )
        )
    return uris


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
