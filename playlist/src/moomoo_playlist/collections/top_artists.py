"""Create playlists based on the top artists in the user's listening history."""

import dataclasses
import os
import random
from pathlib import Path
from uuid import UUID

import click
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..db import db_retry, execute_sql_fetchall, get_session
from ..ddl import PlaylistCollection
from ..generator import FromMbidsPlaylistGenerator, NoFilesRequestedError
from ..logger import get_logger

collection_name = "top-artists"
refresh_interval_hours = 24

logger = get_logger().bind(module=__name__)

# mapping of length to config
HISTORY_CONFIG = {
    "30": dict(col="last30_listen_count", min_n=15),
    "60": dict(col="last60_listen_count", min_n=20),
    "90": dict(col="last90_listen_count", min_n=25),
    "lifetime": dict(col="lifetime_listen_count", min_n=50),
}


@dataclasses.dataclass(frozen=True)
class Artist:
    """Container for artists."""

    mbid: UUID
    name: str

    def __post__init__(self):
        """Post init."""

        # convert mbid to UUID if it's a string
        if isinstance(self.mbid, str):
            self.mbid = UUID(self.mbid)


@db_retry
def list_top_artists(
    username: str, history_length: str, count: int, session: Session
) -> list[Artist]:
    """List the top artists for a user, returning a list of mbids.

    Order is determined by the listen count for the specified history length, in
    descending order.
    """
    logger.info(
        f"Listing top artists for {username} with history length '{history_length}'."
    )
    if history_length not in HISTORY_CONFIG:
        raise ValueError(f"Invalid history length: {history_length}")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    history_column = HISTORY_CONFIG[history_length]["col"]
    min_listen_count = HISTORY_CONFIG[history_length]["min_n"]
    sql = f"""
        select artist_mbid, artist_name
        from {schema}.artist_listen_counts
        where username = :username
          and {history_column} >= :min_listen_count
        order by artist_mbid
    """
    rows = execute_sql_fetchall(
        session=session,
        sql=sql,
        params=dict(username=username, min_listen_count=min_listen_count),
    )
    if len(rows) > count:
        rows = random.sample(rows, count)

    logger.info(f"Found {len(rows)} artists.", artists=[r["artist_name"] for r in rows])
    return [Artist(mbid=row["artist_mbid"], name=row["artist_name"]) for row in rows]


@click.command("top-artists")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
@click.option(
    "--history-length",
    required=True,
    type=click.Choice(list(HISTORY_CONFIG.keys())),
    help="The length of time to consider for the listen count.",
    default="lifetime",
)
@click.option(
    "--count",
    required=True,
    type=click.IntRange(min=1),
    help="The number of playlists to generate.",
    default=15,
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Force refresh of collection, even if not stale.",
)
def main(username: str, history_length: str, count: int, force: bool):
    """Create playlists based on the top artists in the user's listening history."""
    session = get_session()
    collection = PlaylistCollection.get_collection_by_name(
        username=username,
        collection_name=collection_name,
        session=session,
        refresh_interval_hours=refresh_interval_hours,
    )

    if collection.is_fresh and not force:
        logger.info("Collection is not stale; skipping.")
        return

    artists = list_top_artists(
        username=username, history_length=history_length, count=count, session=session
    )

    logger.info(f"Generating playlists for {len(artists)} artists.")
    playlists = []
    for artist in tqdm(artists, disable=None, total=len(artists)):

        generator = FromMbidsPlaylistGenerator(artist.mbid, username=username)
        try:
            playlist = generator.get_playlist(session=session, seed_count=1)
        except NoFilesRequestedError:
            logger.exception(f"No files found for {artist.name}/{artist.mbid}.")
            continue

        # set title based on list index, in case there was an exception
        playlist.title = f"Top Artists {len(playlists) + 1}"
        playlist.description = f"Songs like {artist.name}"
        playlists.append(playlist)

    if len(playlists) == 0:
        logger.warning("No playlists generated.")
        return

    collection.replace_playlists(playlists=playlists, session=session, force=force)


if __name__ == "__main__":
    main()
