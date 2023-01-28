"""Cli group for moomoo enrich."""

import click

from . import annotate_mbids


@click.group()
def cli():
    """Cli group for moomoo ingest."""
    pass


cli.add_command(annotate_mbids.main, "annotate")

if __name__ == "__main__":
    cli()
