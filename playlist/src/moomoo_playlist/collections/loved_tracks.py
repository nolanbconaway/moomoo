"""Create a playlist of the user's loved tracks."""

import os
from pathlib import Path

import click
from sqlalchemy.orm import Session

from moomoo_playlist.db import execute_sql_fetchall, get_session
from moomoo_playlist.ddl import PlaylistCollection

from ..generator import Playlist, Track, db_retry
from ..logger import get_logger

collection_name = "loved-tracks"
logger = get_logger().bind(module=__name__)


@db_retry
def list_loved_tracks(username: str, session: Session) -> list[Path]:
    """List the user's loved tracks, returning a list of Paths.

    Order is loved at time, in descending order.
    """
    logger.info(f"Listing loved tracks for {username}.")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select filepath
        from {schema}.loved_tracks
        where username = :username
        order by love_at desc
    """
    rows = execute_sql_fetchall(
        session=session, sql=sql, params=dict(username=username)
    )

    logger.info(f"Found {len(rows)} tracks.")
    return [Path(row["filepath"]) for row in rows]


@click.command("loved-tracks")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
def main(username: str):
    """Create a playlist of the user's loved tracks."""
    session = get_session()
    paths = list_loved_tracks(username=username, session=session)

    if len(paths) == 0:
        logger.warning("No loved tracks found.")
        return

    logger.info(f"Creating playlist for {len(paths)} loved tracks.")
    playlist = Playlist(
        tracks=[Track(filepath=path) for path in paths],
        title="Loved Tracks",
        description=f"Tracks that {username} has loved on ListenBrainz.",
    )

    collection = PlaylistCollection.get_collection_by_name(
        username=username, collection_name=collection_name, session=session
    )
    collection.replace_playlists(playlists=[playlist], session=session)
    logger.info("Saved playlist to database.")


if __name__ == "__main__":
    main()
