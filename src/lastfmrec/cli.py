"""Cli group for lastfmrec."""

import click

from . import collect_loved_tracks, collect_recent_tracks


@click.group()
def cli():
    """Cli group for lastfmrec."""
    pass


cli.add_command(collect_loved_tracks.main, "ingest-loves")
cli.add_command(collect_recent_tracks.main, "ingest-listens")

if __name__ == "__main__":
    cli()
