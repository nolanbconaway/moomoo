"""Cli group for moomoo ingest."""

import click

from . import collect_listen_data, collect_local_files


@click.group()
def cli():
    """Cli group for moomoo ingest."""
    pass


cli.add_command(collect_listen_data.main, "listens")
cli.add_command(collect_local_files.main, "files")

if __name__ == "__main__":
    cli()
