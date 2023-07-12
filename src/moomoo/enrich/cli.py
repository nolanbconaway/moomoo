"""Cli group for moomoo enrich."""

import click

from . import annotate_mbids, artist_stats


@click.group()
def cli():
    """Cli group for moomoo ingest."""
    pass


cli.add_command(annotate_mbids.main, "annotate")
cli.add_command(artist_stats.main, "artist-stats")

if __name__ == "__main__":
    cli()
