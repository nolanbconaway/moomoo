"""Cli group for moomoo."""

import click

from .ingest import cli as ingest_cli


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


cli.add_command(ingest_cli.cli, "ingest")

if __name__ == "__main__":
    cli()
