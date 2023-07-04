"""Cli group for moomoo."""

from pathlib import Path

import click

from .enrich import cli as enrich_cli
from .ingest import cli as ingest_cli
from .ml import cli as ml_cli
from .playlist import cli as playlist_cli
from .utils_ import moomoo_version


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


@cli.command()
def version():
    """Get the version of moomoo."""
    click.echo(moomoo_version())


cli.add_command(ingest_cli.cli, "ingest")
cli.add_command(enrich_cli.cli, "enrich")
cli.add_command(ml_cli.cli, "ml")
cli.add_command(playlist_cli.cli, "playlist")

if __name__ == "__main__":
    cli()
