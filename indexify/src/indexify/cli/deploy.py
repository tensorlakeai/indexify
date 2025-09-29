import traceback

import click
from tensorlake.applications.remote.deploy import deploy as tl_deploy


@click.command(
    short_help="Deploys application defined in <application-dir-path> directory to Indexify"
)
@click.argument(
    "application-dir-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "-u",
    "--upgrade-running-requests",
    is_flag=True,
    default=False,
    help="Upgrade requests that are already queued or running to use the new deployed version of the application",
)
def deploy(
    application_dir_path: str,
    upgrade_running_requests: bool,
):
    click.echo(f"Preparing deployment for application from {application_dir_path}")

    try:
        tl_deploy(
            application_source_dir_or_file_path=application_dir_path,
            upgrade_running_requests=upgrade_running_requests,
            load_application_modules=True,
        )
    except Exception as e:
        click.secho(
            f"Application could not be deployed, please check the error message:",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    click.secho(f"Successfully deployed the application", fg="green")
