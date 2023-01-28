"""Cli group for moomoo."""

from pathlib import Path

import click

from .enrich import cli as enrich_cli
from .ingest import cli as ingest_cli


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


@cli.command()
def version():
    """Get the version of moomoo."""
    click.echo((Path(__file__).resolve().parent / "version").read_text().strip())


cli.add_command(ingest_cli.cli, "ingest")
cli.add_command(enrich_cli.cli, "enrich")

if __name__ == "__main__":
    cli()
