import os
import traceback

import click
from tensorlake.applications.remote.deploy import deploy_applications


@click.command(
    short_help="Deploys applications defined in <application-file-path> .py file to Indexify"
)
@click.argument(
    "application-file-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
@click.option(
    "-u",
    "--upgrade-running-requests",
    is_flag=True,
    default=False,
    help="Upgrade requests that are already queued or running to use the new deployed version of the applications",
)
def deploy(
    application_file_path: str,
    upgrade_running_requests: bool,
):
    click.echo(f"Preparing deployment for applications from {application_file_path}")

    try:
        deploy_applications(
            applications_file_path=application_file_path,
            upgrade_running_requests=upgrade_running_requests,
            load_source_dir_modules=True,
        )
    except Exception as e:
        click.secho(
            f"Applications could not be deployed, please check the error message:",
            fg="red",
        )
        traceback.print_exception(e)
        raise click.Abort

    click.secho(f"Successfully deployed the applications", fg="green")
