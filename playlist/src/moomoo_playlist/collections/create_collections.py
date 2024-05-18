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

nighttime_nyc_hour = 7  # converted from middle of the night in NYC to UTC

refresh_hours = {
    loved_tracks_collection_name: None,
    revisit_tracks_collection_name: None,
    revisit_releases_collection_name: [nighttime_nyc_hour],
    smart_mix_collection_name: [nighttime_nyc_hour],
    top_artists_collection_name: [nighttime_nyc_hour],
}


def create_collections(username: str, session: Session, replace: bool = False) -> None:
    """Create collections for the given user if they do not exist."""
    for collection_name, refresh_at_hours_utc in refresh_hours.items():
        try:
            collection = PlaylistCollection.get_collection_by_name(
                username=username, collection_name=collection_name, session=session
            )
        except ValueError:
            collection = None

        # continue if collection exists and we are not replacing
        if collection and not replace:
            click.echo(
                f"Collection '{collection_name}' for user '{username}' already exists, "
                + "skipping."
            )
            continue

        # if here and collection exists, drop it
        if collection:
            click.echo(f"Dropping '{collection_name}' for user '{username}'.")
            session.delete(collection)
            session.commit()

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
@click.option(
    "--replace",
    is_flag=True,
    help="Replace existing collections if they exist.",
)
def main(username: str, replace: bool):
    """Create collections for the given user if they do not exist."""
    with get_session() as session:
        create_collections(username=username, session=session, replace=replace)
    click.echo("Done.")
