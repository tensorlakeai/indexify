import multiprocessing
import os
import sys
from typing import Optional

import typer
from typing_extensions import Annotated

from . import indexify_executor, version

typer_app = typer.Typer(
    help="indexify-executor - CLI for running indexify executor",
    pretty_exceptions_enable=False,
)

class Unbuffered(object):
    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


sys.stdout = Unbuffered(sys.stdout)


def print_version():
    print(f"indexify-extractor-sdk version {version.__version__}")

@typer_app.command(help="Joins the extractors to the coordinator server")
def start(
    server_addr: str = "localhost:8900",
    workers: Annotated[
        int, typer.Option(help="number of worker processes for extraction")
    ] = 1,
    config_path: Optional[str] = typer.Option(
        None, help="Path to the TLS configuration file"
    ),
):
    print_version()

    print("workers ", workers)
    print("config path provided ", config_path)

    indexify_executor.join(
        workers=workers,
        server_addr=server_addr,
        config_path=config_path,
    )


@typer_app.command(name="--version", help="Print the version of the SDK and the CLI")
def _version():
    print_version()
