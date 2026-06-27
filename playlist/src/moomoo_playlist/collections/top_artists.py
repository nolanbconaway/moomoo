"""Create playlists based on the top artists in the user's listening history."""

import os
import random
from uuid import UUID

import click
from moomoo_pg import (
    Playlist,
    PlaylistCollection,
    db_retry,
    execute_sql_fetchall,
    get_session,
)
from pydantic import BaseModel
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..generator import FromMbidsPlaylistGenerator, NoFilesRequestedError
from ..generator.base import SPECIAL_PURPOSE_ARTISTS
from ..logger import get_logger

collection_name = "top-artists"
logger = get_logger().bind(module=__name__)

RECENCY_FAC = 0.5

# mapping of length to config
HISTORY_CONFIG = {
    "30": dict(col="last30_listen_count", min_n=15),
    "60": dict(col="last60_listen_count", min_n=20),
    "90": dict(col="last90_listen_count", min_n=25),
    "lifetime": dict(col="lifetime_listen_count", min_n=50),
}


class Artist(BaseModel):
    """Container for artists."""

    mbid: UUID
    name: str


@db_retry
def list_top_artists(
    username: str, history_length: str, count: int, session: Session
) -> list[Artist]:
    """List the top artists for a user, returning a list of mbids.

    Order is determined by the listen count for the specified history length, in
    descending order.
    """
    logger.info(f"Listing top artists for {username} with history length '{history_length}'.")
    if history_length not in HISTORY_CONFIG:
        raise ValueError(f"Invalid history length: {history_length}")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    history_column = HISTORY_CONFIG[history_length]["col"]
    min_listen_count = HISTORY_CONFIG[history_length]["min_n"]
    sql = f"""
        select
            artist_mbid as mbid
            , artist_name as name
        from {schema}.artist_listen_counts
        where username = :username
          and {history_column} >= :min_listen_count
          and artist_mbid != any(:special_purpose_artists)
        order by artist_mbid
    """
    rows = execute_sql_fetchall(
        session=session,
        sql=sql,
        params=dict(
            username=username,
            min_listen_count=min_listen_count,
            special_purpose_artists=list(SPECIAL_PURPOSE_ARTISTS),
        ),
    )
    if len(rows) > count:
        rows = random.sample(rows, count)

    logger.info(f"Found {len(rows)} artists.", artists=[r["name"] for r in rows])
    return [Artist(**row) for row in rows]


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
    with get_session() as session:
        collection = PlaylistCollection.get_collection_by_name(
            username=username, collection_name=collection_name, session=session
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
                tracks = generator.get_tracks(
                    session=session, seed_count=1, recency_fac=RECENCY_FAC
                )
            except NoFilesRequestedError:
                logger.exception(f"No files found for {artist.name}/{artist.mbid}.")
                continue

            # set title based on list index, in case there was an exception
            playlist = Playlist.Data(
                title=f"Top Artists {len(playlists) + 1}",
                description=f"Songs like {artist.name}",
                tracks=tracks,
            )
            playlists.append(playlist)

        if len(playlists) == 0:
            logger.warning("No playlists generated.")
            return

        collection.replace_playlists(playlists=playlists, session=session, force=force)
        session.commit()
        logger.info(f"Saved {len(playlists)} playlist(s) to database.")


if __name__ == "__main__":
    main()
