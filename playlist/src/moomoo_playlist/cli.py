"""Command line interface for moomoo_playlist."""

import click

from .collections.revisit_releases import main as revisit_releases_main
from .collections.top_artists import main as top_artists_main
from .db import get_session
from .ddl import BaseTable


@click.group()
def cli():
    """Create playlists based on your music library."""
    pass


@cli.command("create-db")
def create_db():
    """Create the database."""
    with get_session() as session:
        engine = session.get_bind()
        BaseTable.metadata.create_all(engine)


cli.add_command(top_artists_main)
cli.add_command(revisit_releases_main)

if __name__ == "__main__":
    cli()
