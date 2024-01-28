"""Create playlists for specific releases to revisit based on the user's listening."""
import dataclasses
import datetime
import os
import random
from uuid import UUID

import click
from sqlalchemy.orm import Session
from tqdm import tqdm

from moomoo_playlist.db import execute_sql_fetchall, get_session
from moomoo_playlist.ddl import PlaylistCollection

from ..generator import NoFilesRequestedError, QueryPlaylistGenerator, db_retry
from ..logger import get_logger

collection_name = "revisit-releases"
logger = get_logger().bind(module=__name__)


@dataclasses.dataclass(frozen=True)
class Release:
    """Container for artists."""

    mbid: UUID
    title: str
    artist_name: str

    def __post__init__(self):
        """Post init."""

        # convert mbid to UUID if it's a string
        if isinstance(self.mbid, str):
            self.mbid = UUID(self.mbid)


@db_retry
def list_revisit_releases(username: str, count: int, session: Session) -> list[Release]:
    """List releases to revisit.

    Order is random.
    """
    logger.info(f"Listing {count} revisit releases for {username}.")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select release_group_mbid, release_group_title, artist_name
        from {schema}.revisit_releases
        where username = :username
        order by release_group_mbid
    """
    rows = execute_sql_fetchall(
        session=session, sql=sql, params=dict(username=username)
    )

    if len(rows) > count:
        # select random rows. set the seed to be constant for a given date so that
        # refreshes of the playlist will be consistent. maybe one day use skip logic
        # based on recency instead of a crazy random seed.
        date = datetime.date.today()
        random.seed(date.year + date.month + date.day)
        rows = random.sample(rows, count)

    logger.info(f"Found {len(rows)} releases.", extra=dict(releases=rows))
    res = [
        Release(
            mbid=row["release_group_mbid"],
            title=row["release_group_title"],
            artist_name=row["artist_name"],
        )
        for row in rows
    ]

    return sorted(res, key=lambda x: (x.artist_name, x.title))


@click.command("revisit-releases")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
@click.option(
    "--count",
    required=True,
    type=click.IntRange(min=1),
    help="The number of playlists to generate.",
    default=10,
)
def main(username: str, count: int):
    """Create playlists based on the top artists in the user's listening history."""
    session = get_session()
    schema = os.environ["MOOMOO_DBT_SCHEMA"]

    releases = list_revisit_releases(username=username, count=count, session=session)

    logger.info(f"Generating playlists for {len(releases)} releases.")
    sql = f"""
        select filepath
        from {schema}.map__file_release_group
        where release_group_mbid=:mbid
        order by filepath
    """

    playlists = []
    for i, release in tqdm(enumerate(releases, 1), disable=None, total=len(releases)):
        generator = QueryPlaylistGenerator(sql, params=dict(mbid=release.mbid))
        try:
            playlist = generator.get_playlist(session=session)
        except NoFilesRequestedError:
            logger.exception(f"No files found for release mbid={release.mbid}.")
            continue

        playlist.title = f"Revisit Release {i}"
        playlist.description = f"Revisit: {release.title} - {release.artist_name}"
        playlists.append(playlist)

    if len(playlists) == 0:
        logger.warning("No playlists generated.")
        return

    logger.info(f"Saving {len(playlists)} playlists to database.")
    PlaylistCollection.save_collection(
        playlists=playlists,
        username=username,
        collection_name=collection_name,
        session=session,
    )
    logger.info("Saved playlists to database.")


if __name__ == "__main__":
    main()
