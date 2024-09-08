"""Cli handlers for moomoo machine learning."""

from pathlib import Path

import click

from .conditioner.cli import cli as conditioner_cli
from .db import BaseTable, FileEmbedding, LocalFileExcludeRegex, get_session
from .scorer.cli import cli as scorer_cli

VERSION = (Path(__file__).parent / "version").read_text().strip()


@click.group()
def cli():
    """Cli group for moomoo ml."""
    pass


@cli.command("version")
def version():
    """Print the version."""
    click.echo(VERSION)


@cli.command("create-db")
def create_db():
    """Create the database."""
    with get_session() as session:
        engine = session.get_bind()
        BaseTable.metadata.create_all(
            engine, tables=[FileEmbedding.__table__, LocalFileExcludeRegex.__table__]
        )


cli.add_command(conditioner_cli, name="conditioner")
cli.add_command(scorer_cli, name="scorer")

if __name__ == "__main__":
    cli()
