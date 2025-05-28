from tensorlake.utils.logging import (
    configure_development_mode_logging,
    configure_logging_early,
    configure_production_mode_logging,
)

configure_logging_early()

import shutil
from importlib.metadata import version
from pathlib import Path
from socket import gethostname
from typing import Dict, List, Optional

import click
import nanoid
import prometheus_client
import structlog

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


@click.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    short_help="Runs Executor that connects to the Indexify server and starts running its tasks",
)
@click.option(
    "--server-addr",
    "server_address",
    default="localhost:8900",
    help="Address of Indexify HTTP Server to connect to",
)
@click.option(
    "--grpc-server-addr",
    "grpc_server_address",
    default="localhost:8901",
    help="Address of Indexify gRPC Server to connect to",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Verbose logging",
)
@click.option(
    "-vv",
    "--very-verbose",
    is_flag=True,
    default=False,
    help="Very verbose logging",
)
@click.option(
    "-f",
    "--function",
    "function_uris",
    default=[],
    multiple=True,
    help="Functions that the executor will run "
    "specified as <namespace>:<workflow>:<function>:<version>"
    "version is optional, not specifying it will make the server send any version"
    "of the function. Any number of --function arguments can be passed.",
)
@click.option(
    "--config-path",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True),
    default=None,
    help="Path to the TLS configuration file",
)
@click.option(
    "--executor-cache-path",
    type=click.Path(exists=False, dir_okay=True, readable=True, writable=True),
    default="~/.indexify/executor_cache",
    help="Path to the executor cache directory",
)
@click.option(
    "--monitoring-server-host",
    default="localhost",
    help="IP address or hostname where to run Executor Monitoring server",
)
@click.option(
    "--monitoring-server-port",
    default=7000,
    type=int,
    help="Port where to run Executor Monitoring server",
)
@click.option(
    "-l",
    "--label",
    "labels",
    default=[],
    multiple=True,
    help="Executor key-value label to be sent to the Server. "
    "Specified as <key>=<value>",
)
@click.pass_context
def executor(
    ctx: click.Context,
    server_address: str,
    grpc_server_address: str,
    verbose: bool,
    very_verbose: bool,
    function_uris: List[str],
    config_path: Optional[str],
    executor_cache_path: str,
    monitoring_server_host: str,
    monitoring_server_port: int,
    labels: List[str],
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
        server_address=server_address,
        grpc_server_address=grpc_server_address,
        config_path=config_path,
        executor_version=executor_version,
        labels=kv_labels,
        executor_cache_path=executor_cache_path,
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

    executor_cache_path: Path = Path(executor_cache_path).expanduser().absolute()
    if executor_cache_path.exists():
        shutil.rmtree(str(executor_cache_path))
    executor_cache_path.mkdir(parents=True, exist_ok=True)

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
        cache_path=executor_cache_path,
        function_uris=function_uris,
        function_executor_server_factory=SubprocessFunctionExecutorServerFactory(
            verbose_logs=verbose or very_verbose
        ),
        server_addr=server_address,
        grpc_server_addr=grpc_server_address,
        config_path=config_path,
        monitoring_server_host=monitoring_server_host,
        monitoring_server_port=monitoring_server_port,
        blob_store=blob_store,
        host_resources_provider=host_resources_provider,
    ).run()
