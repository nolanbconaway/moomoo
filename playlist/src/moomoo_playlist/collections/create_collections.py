"""Define top-level registered collections and create them if they do not exist."""

import click
from sqlalchemy.orm import Session

from ..db import get_session
from ..ddl import PlaylistCollection
from .loved_tracks import collection_name as loved_tracks_collection_name
from .revisit_releases import collection_name as revisit_releases_collection_name
from .revisit_tracks import collection_name as revisit_tracks_collection_name
from .smart_mix import collection_name as smart_mix_collection_name
from .top_artists import collection_name as top_artists_collection_name

eightam_nyc_hour = 12

refresh_hours = {
    loved_tracks_collection_name: None,
    revisit_tracks_collection_name: None,
    revisit_releases_collection_name: [eightam_nyc_hour],
    smart_mix_collection_name: [eightam_nyc_hour],
    top_artists_collection_name: [eightam_nyc_hour],
}


def create_collections(username: str, session: Session) -> None:
    """Create collections for the given user if they do not exist."""
    for collection_name, refresh_at_hours_utc in refresh_hours.items():
        try:
            PlaylistCollection.get_collection_by_name(
                username=username, collection_name=collection_name, session=session
            )
            click.echo(
                f"Collection '{collection_name}' for user '{username}' already exists."
            )
        except ValueError:
            click.echo(
                f"Creating collection '{collection_name}' for user '{username}'."
            )

            collection = PlaylistCollection(
                username=username,
                collection_name=collection_name,
                refresh_at_hours_utc=refresh_at_hours_utc,
            )
            session.add(collection)
            session.commit()
            click.echo(f"Created collection '{collection_name}' for user '{username}'.")


@click.command("create-collections")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
def main(username: str):
    """Create collections for the given user if they do not exist."""
    with get_session() as session:
        create_collections(username=username, session=session)
    click.echo("Done.")
