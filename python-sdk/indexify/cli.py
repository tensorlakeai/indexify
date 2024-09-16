import io
import asyncio
import docker
import typer
from rich import print

from indexify import Graph
from indexify.functions_sdk.image import Image
from typing import Annotated, Optional
import nanoid
from indexify.executor.function_worker import FunctionWorker
from indexify.executor.agent import ExtractorAgent

app = typer.Typer(pretty_exceptions_enable=False, no_args_is_help=True)


@app.command()
def build_image(workflow_file_path: str, func_name: str = None):
    globals_dict = {"__name__": "__main__"}

    try:
        exec(open(workflow_file_path).read(), globals_dict)
    except FileNotFoundError as e:
        raise Exception(
            f"could not find workflow file to execute at: " f"`{workflow_file_path}`"
        )

    graph = None
    for name, obj in globals_dict.items():
        if type(obj) is Graph:
            print(f"Found graph: `{obj.name}`")
            graph = obj
            break

    if graph is None:
        raise Exception(f"could not find a graph object in file `{workflow_file_path}`")

    if func_name is None:
        print(f"Creating containers for all functions.")
        for func_name in obj.nodes:
            _create_image_for_func(obj, func_name)
    else:
        _create_image_for_func(obj, func_name)


@app.command(help="Joins the extractors to the coordinator server")
def executor(
    server_addr: str = "localhost:8900",
    workers: Annotated[
        int, typer.Option(help="number of worker processes for extraction")
    ] = 1,
    config_path: Optional[str] = typer.Option(
        None, help="Path to the TLS configuration file"
    ),
):
    print("workers ", workers)
    print("config path provided ", config_path)
    print(f"receiving tasks from server addr: {server_addr}")
    id = nanoid.generate()
    print(f"executor id: {id}")

    function_worker = FunctionWorker(workers=workers)

    agent = ExtractorAgent(
        id,
        num_workers=workers,
        function_worker=function_worker,
        server_addr=server_addr,
        config_path=config_path,
    )

    try:
        asyncio.get_event_loop().run_until_complete(agent.run())
    except asyncio.CancelledError as ex:
        print("exiting gracefully", ex)


def _create_image_for_func(g, func_name):
    print(f"creating container for `{func_name}`.")

    if func_name not in g.nodes.keys():
        raise Exception(f"could not find `{func_name}` in graph {g.name}")

    node = g.nodes[func_name]
    _build_image(image=node.image, func_name=func_name)


def _build_image(image: Image, func_name: str = None):
    try:
        client = docker.from_env()
        client.ping()
    except Exception as e:
        print(f"unable to connect with docker: {e}")
        exit(-1)

    docker_file_str_template = """
        FROM {base_image}

        WORKDIR /app

        """

    docker_file_str = docker_file_str_template.format(base_image=image._base_image)

    docker_file_str += "\n".join(image._run_strs)

    print("[bold]creating image using Dockerfile contents,[/bold]")
    print(f"[magenta]{docker_file_str}[/magenta]\n\n")

    client = docker.from_env()
    client.images.build(
        fileobj=io.BytesIO(docker_file_str.encode()),
        tag=image._tag,
        rm=True,
    )
