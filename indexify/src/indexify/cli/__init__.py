import click

from . import build_image, deploy, executor


@click.group()
@click.version_option(package_name="indexify", prog_name="indexify-cli")
@click.pass_context
def cli(ctx: click.Context):
    """
    Indexify CLI to manage and deploy workflows to Indexify Server and run Indexify Executors.
    """
    pass


cli.add_command(build_image.build_image)
cli.add_command(deploy.deploy)
cli.add_command(executor.executor)
