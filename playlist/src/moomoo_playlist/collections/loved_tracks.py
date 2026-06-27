"""Create a playlist of the user's loved tracks."""

import os

import click
from moomoo_pg import (
    Playlist,
    PlaylistCollection,
    PlaylistTrack,
    db_retry,
    execute_sql_fetchall,
    get_session,
)
from sqlalchemy.orm import Session

from ..logger import get_logger

collection_name = "loved-tracks"
logger = get_logger().bind(module=__name__)


@db_retry
def list_loved_tracks(username: str, session: Session) -> list[PlaylistTrack.Data]:
    """List the user's loved tracks, returning a list of PlaylistTrack.Data.

    Order is loved at time, in descending order.
    """
    logger.info(f"Listing loved tracks for {username}.")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select filepath, local_files.track_length_seconds
        from {schema}.loved_tracks
        inner join {schema}.local_files using (filepath)
        where username = :username
        order by love_at desc
    """
    rows = execute_sql_fetchall(session=session, sql=sql, params=dict(username=username))
    logger.info(f"Found {len(rows)} tracks.")
    return [PlaylistTrack.Data(**row) for row in rows]


@click.command("loved-tracks")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
def main(username: str):
    """Create a playlist of the user's loved tracks."""
    with get_session() as session:
        tracks = list_loved_tracks(username=username, session=session)

        if len(tracks) == 0:
            logger.warning("No loved tracks found.")
            return

        logger.info(f"Creating playlist for {len(tracks)} loved tracks.")
        playlist = Playlist.Data(
            tracks=tracks,
            title="Loved Tracks",
            description=f"Tracks that {username} has loved on ListenBrainz.",
        )
        collection = PlaylistCollection.get_collection_by_name(
            username=username, collection_name=collection_name, session=session
        )
        collection.replace_playlists(playlists=[playlist], session=session)
        session.commit()

    logger.info("Saved playlist to database.")


if __name__ == "__main__":
    main()
