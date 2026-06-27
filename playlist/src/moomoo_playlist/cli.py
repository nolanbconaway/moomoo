"""Command line interface for moomoo_playlist."""

from pathlib import Path

import click

from .collections.create_collections import main as create_collections_main
from .collections.loved_tracks import main as loved_tracks_main
from .collections.revisit_releases import main as revisit_releases_main
from .collections.revisit_tracks import main as revisit_tracks_main
from .collections.smart_mix import main as smart_mix_main
from .collections.top_artists import main as top_artists_main


@click.group()
def cli():
    """Create playlists based on your music library."""
    pass


@cli.command("version")
def version():
    """Print the version number."""
    p = Path(__file__).parent / "version"
    click.echo(p.read_text().strip())


cli.add_command(create_collections_main)
cli.add_command(top_artists_main)
cli.add_command(revisit_releases_main)
cli.add_command(loved_tracks_main)
cli.add_command(smart_mix_main)
cli.add_command(revisit_tracks_main)

if __name__ == "__main__":
    cli()
