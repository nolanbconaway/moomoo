"""Cli group for moomoo."""

import click

from .ingest import cli as ingest_cli
from .enrich import cli as enrich_cli


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


cli.add_command(ingest_cli.cli, "ingest")
cli.add_command(enrich_cli.cli, "enrich")

if __name__ == "__main__":
    cli()
