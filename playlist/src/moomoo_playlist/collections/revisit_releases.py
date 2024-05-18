"""Create playlists for specific releases to revisit based on the user's listening."""

import dataclasses
import os
from uuid import UUID

import click
import numpy as np
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..db import db_retry, execute_sql_fetchall, get_session
from ..ddl import PlaylistCollection
from ..generator import NoFilesRequestedError, QueryPlaylistGenerator
from ..logger import get_logger

collection_name = "revisit-releases"
logger = get_logger().bind(module=__name__)


@dataclasses.dataclass
class Release:
    """Container for artists."""

    mbid: UUID
    title: str
    artist_name: str
    revisit_score: float

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
        select release_group_mbid, release_group_title, artist_name, revisit_score
        from {schema}.revisit_releases
        where username = :username
        order by release_group_mbid
    """
    rows = execute_sql_fetchall(
        session=session, sql=sql, params=dict(username=username)
    )

    if len(rows) > count:
        # sample relative to exp(revisit score)
        logger.info(f"Sampling down to {count} releases from {len(rows)}.")
        max_score = max(row["revisit_score"] for row in rows)
        scores = np.exp([max_score - row["revisit_score"] for row in rows])
        scores /= scores.sum()

        idx = np.random.choice(range(len(rows)), size=count, replace=False, p=scores)
        rows = sorted(
            [rows[i] for i in idx], key=lambda x: x["revisit_score"], reverse=True
        )

    logger.info(f"Found {len(rows)} releases.", extra=dict(releases=rows))
    res = [
        Release(
            mbid=row["release_group_mbid"],
            title=row["release_group_title"],
            artist_name=row["artist_name"],
            revisit_score=row["revisit_score"],
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
    default=15,
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Force refresh of collection, even if not stale.",
)
def main(username: str, count: int, force: bool):
    """Create playlists based on the top artists in the user's listening history."""
    session = get_session()
    collection = PlaylistCollection.get_collection_by_name(
        username=username, collection_name=collection_name, session=session
    )

    if collection.is_fresh and not force:
        logger.info("Collection is not stale; skipping.")
        return

    releases = list_revisit_releases(username=username, count=count, session=session)

    logger.info(f"Generating playlists for {len(releases)} releases.")
    sql = f"""
        select filepath
        from {os.environ["MOOMOO_DBT_SCHEMA"]}.map__file_release_group
        where release_group_mbid=:mbid
        order by filepath
    """

    playlists = []
    for release in tqdm(releases, disable=None, total=len(releases)):
        generator = QueryPlaylistGenerator(sql, params=dict(mbid=release.mbid))
        try:
            playlist = generator.get_playlist(session=session)
        except NoFilesRequestedError:
            logger.exception(f"No files found for release mbid={release.mbid}.")
            continue

        # set title based on list index, in case there was an exception
        playlist.title = f"Revisit Release {len(playlists) + 1}"
        playlist.description = f"Revisit: {release.title} - {release.artist_name}"
        playlists.append(playlist)

    if len(playlists) == 0:
        logger.warning("No playlists generated.")
        return

    collection.replace_playlists(playlists=playlists, session=session, force=force)


if __name__ == "__main__":
    main()
