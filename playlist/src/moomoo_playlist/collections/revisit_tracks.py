"""Create a playlist of tracks for the user to revisit."""

import os
import random
from collections import Counter
from pathlib import Path

import click
from sqlalchemy.orm import Session

from ..db import db_retry, execute_sql_fetchall, get_session
from ..ddl import PlaylistCollection
from ..logger import get_logger
from ..playlist import Playlist, Track

collection_name = "revisit-tracks"
logger = get_logger().bind(module=__name__)


@db_retry
def list_revisit_tracks(username: str, session: Session) -> list[Track]:
    """Get an ordered list of tracks to revisit.

    Guraranteed to be unique on recording, but may have repeat filepath. Ordered by
    revisit score (most to least).
    """
    logger.info(f"Listing revisit tracks for {username}.")

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
    select filepath, recording_mbid, artist_mbid, album_artist_mbid
    from {schema}.revisit_tracks
    where username = :username
    order by revisit_score desc, recording_mbid
    limit 1000
    """
    rows = execute_sql_fetchall(
        session=session, sql=sql, params=dict(username=username)
    )

    logger.info(f"Found {len(rows)} tracks.")
    return [
        Track(
            filepath=Path(row["filepath"]),
            recording_mbid=row["recording_mbid"],
            artist_mbid=row["artist_mbid"],
            album_artist_mbid=row["album_artist_mbid"] or row["artist_mbid"],
        )
        for row in rows
    ]


def create_playlist(tracks: list[Track], total_tracks: int) -> Playlist:
    """Create a playlist from the given tracks.

    This function needs to do all the deduplicating and ordering. The dedupe is:

    1. One track per recording_mbid.
    2. One track per filepath.
    3. Two tracks per artist (artist and album artist each count).
    """

    # we have multiple tracks per recording, but should hav
    counter = Counter()
    consumed_tracks = []

    for track in tracks:
        if (
            counter[track.recording_mbid] == 0
            and counter[track.filepath] == 0
            and counter[track.artist_mbid] < 2
            and counter[track.album_artist_mbid] < 2
        ):
            consumed_tracks.append(track)
            counter[track.recording_mbid] += 1
            counter[track.filepath] += 1
            counter[track.artist_mbid] += 1
            if track.album_artist_mbid != track.artist_mbid:
                counter[track.album_artist_mbid] += 1

        if len(consumed_tracks) == total_tracks:
            break

    # TODO: manage ordering. currently shuffle but eventually should be smart. The goal
    # is to have similar tracks close together.
    random.shuffle(consumed_tracks)

    return Playlist(tracks=consumed_tracks)


@click.command("revisit-tracks")
@click.argument("username", required=True, envvar="LISTENBRAINZ_USERNAME")
def main(username: str):
    """Create a playlist of the user's loved tracks."""
    session = get_session()
    tracks = list_revisit_tracks(username=username, session=session)

    if len(tracks) == 0:
        logger.warning("No revisit tracks found.")
        return

    logger.info(f"Creating playlist for {len(tracks)} tracks.")
    playlist = create_playlist(tracks=tracks, total_tracks=20)
    playlist.title = "Revisit Tracks"
    playlist.description = f"Tracks for {username} to revisit."

    collection = PlaylistCollection.get_collection_by_name(
        username=username, collection_name=collection_name, session=session
    )
    collection.replace_playlists(playlists=[playlist], session=session)
    logger.info("Saved playlist to database.")


if __name__ == "__main__":
    main()
