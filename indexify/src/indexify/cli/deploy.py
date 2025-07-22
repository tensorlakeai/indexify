import click
from tensorlake import Graph
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.functions_sdk.remote_graph import RemoteGraph
from tensorlake.functions_sdk.workflow_module import (
    WorkflowModuleInfo,
    load_workflow_module_info,
)


@click.command(
    short_help="Deploy all graphs/workflows defined in the workflow file to Indexify"
)
# Path to the file where the graphs/workflows are defined as global variables
@click.argument(
    "workflow-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "-u",
    "--upgrade-queued-requests",
    is_flag=True,
    default=False,
    help="Upgrade invocations that are already queued or running to use the deployed version of the graphs/workflows",
)
def deploy(
    workflow_file_path: str,
    upgrade_queued_invocations: bool,
):
    click.echo(f"Preparing deployment for {workflow_file_path}")
    try:
        workflow_module_info: WorkflowModuleInfo = load_workflow_module_info(
            workflow_file_path
        )
    except Exception as e:
        click.secho(
            f"Failed loading workflow file, please check the error message: {e}",
            fg="red",
        )
        raise click.Abort

    for graph in workflow_module_info.graphs:
        graph: Graph
        try:
            RemoteGraph.deploy(
                graph,
                code_dir_path=graph_code_dir_path(workflow_file_path),
                upgrade_tasks_to_latest_version=upgrade_queued_invocations,
            )
        except Exception as e:
            click.secho(
                f"Graph {graph.name} could not be deployed, please check the error message: {e}",
                fg="red",
            )
            raise click.Abort

        click.secho(f"Deployed {graph.name}", fg="green")
