"""Cli group for moomoo."""

import click

from .db.cli import cli as db_cli

try:
    # needs ingest deps
    from .enrich import cli as enrich_cli
except ImportError:
    enrich_cli = None

try:
    # needs ingest deps
    from .ingest import cli as ingest_cli
except ImportError:
    ingest_cli = None

try:
    # needs ml deps
    from .ml import cli as ml_cli
except ImportError:
    ml_cli = None

from .utils_ import moomoo_version


@click.group()
def cli():
    """Cli group for moomoo."""
    pass


@cli.command()
def version():
    """Get the version of moomoo."""
    click.echo(moomoo_version())


cli.add_command(db_cli, "db")

if ingest_cli is not None:
    cli.add_command(ingest_cli.cli, "ingest")
if enrich_cli is not None:
    cli.add_command(enrich_cli.cli, "enrich")
if ml_cli is not None:
    cli.add_command(ml_cli.cli, "ml")

if __name__ == "__main__":
    cli()
