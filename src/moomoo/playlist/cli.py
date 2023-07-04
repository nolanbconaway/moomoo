"""Cli handlers for moomoo playlists."""

import click
from . import playlist_from_file


@click.group()
def cli():
    """Cli group for moomoo playlist."""
    pass


cli.add_command(playlist_from_file.cli, "from-file")

if __name__ == "__main__":
    cli()
