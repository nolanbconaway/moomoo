"""Base utilties for playlist generation."""

import abc
import os
from collections import Counter
from math import log
from pathlib import Path
from typing import Generator, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import db_retry, execute_sql_fetchall, make_temp_table
from ..playlist import Playlist, Track

# Special purpose artists are artists that are used for a special purpose, such as
# "Various Artists" for compilations. They are not real artists, so should pass thru
# max artist count logic, etc.
#
# Docs: https://musicbrainz.org/doc/Style/Unknown_and_untitled/Special_purpose_artist
SPECIAL_PURPOSE_ARTISTS = {
    UUID("f731ccc4-e22a-43af-a747-64213329e088"),  # anonymous
    UUID("33cf029c-63b0-41a0-9855-be2a3665fb3b"),  # data
    UUID("314e1c25-dde7-4e4d-b2f4-0a7b9f7c56dc"),  # dialogue
    UUID("eec63d3c-3b81-4ad4-b1e4-7c147d4d2b61"),  # no artist
    UUID("9be7f096-97ec-4615-8957-8d40b5dcbc41"),  # traditional
    UUID("125ec42a-7229-4250-afc5-e057484327fe"),  # unknown
    UUID("89ad4ac3-39f7-470e-963a-56509c546377"),  # various artists
}

# A list of mashup artists. These artists appear in basically all playists because they
# have a slice of everything in them. So do not put them in any playlist for now.
#
# Eventually I will want a better playlist generator which is sensitive to this context.
MASHUP_ARTISTS = {
    UUID("24e36781-1f4a-40af-bd18-c5de61f10c66"),  # girl talk
}

# A list of recording mbids that appear all over the place for some reason. The system
# should not include these in playlists.
EARWORMS = {
    UUID("27f83ca8-bce1-4643-99ac-3877cc4984a4"),
    UUID("a7974276-20a6-4438-975b-3328fbf81668"),
    UUID("0eaf0546-0a86-419d-b7cb-fac6a77eb55a"),
    UUID("666148da-9889-470c-a2cd-efc9fb0c7199"),
    UUID("bdc55bab-f300-42f9-b2dc-10f035e536a9"),
    UUID("f12b4b53-d5ba-45b8-b074-b840199db707"),
    UUID("0a16e140-4767-4da4-87c1-ae32b0321777"),
}


class NoFilesRequestedError(Exception):
    """No files requested by the user."""

    pass


class BasePlaylistGenerator(abc.ABC):
    """Base class for playlist generators.

    Subclasses should implement get_playlist. Mostly here for type hinting with mixed
    types of playlist generators.
    """

    @abc.abstractmethod
    def get_playlist(self) -> Playlist: ...

    @staticmethod
    def listen_count_to_weight(x: int) -> float:
        """Convert a listen count to a weight.

        This is a simple logarithmic function which converts a listen count to a non-
        zero weight via log(2 + x). I put it here to share across playlist generators.

        Args:
            listen_count: Listen count.

        Returns:
            A weight for use in similarity calculations.
        """
        return log(2 + max(x, 0))


@db_retry
def fetch_user_listen_counts(
    filepaths: list[Path],
    session: Session,
    username: str,
    history_column: str = "lifetime_listen_count",
) -> dict[Path, int]:
    """Fetch the listen counts for a user's tracks.

    Args:
        filepaths: List of filepaths.
        session: Sqlalchemy session.
        username: Username to fetch listen counts for.
        history_column: Column to fetch listen counts from.

    Returns:
        A dictionary of filepaths to listen counts.
    """
    if not filepaths:
        return dict()

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    tmp_name = make_temp_table(
        session=session,
        types={"filepath": "text"},
        data=[{"filepath": str(f)} for f in filepaths],
        pk="filepath",
    )
    sql = f"""
        select filepath, {history_column} as listens
        from {schema}.file_listen_counts
        inner join {tmp_name} using (filepath)
        where username = :username
    """
    rows = {
        Path(row["filepath"]): row["listens"]
        for row in execute_sql_fetchall(
            sql=sql, params={"username": username}, session=session
        )
    }

    # add any missing filepaths with 0 listens
    return {fp: rows.get(fp, 0) for fp in filepaths}


@db_retry
def stream_similar_tracks(
    filepaths: list[Path],
    session: Session,
    limit: Optional[int] = 2000,
    weights: Optional[list[float]] = None,
) -> Generator[Track, None, None]:
    """Stream similar tracks to filepaths.

    This is best used internally, as it does not return a list of filepaths, but rather
    a generator of PlaylistTrack objects. Consumers will need to iterate over the
    generator to and implement logic to limit the number of songs per artist, etc.

    Args:
        filepaths: List of paths files.
        session: Sqlalchemy session.
        limit: Maximum number of tracks to return (default 500).
        weights: List of weights for each filepath. If provided, the aggregate distance
            will be weighted (larger weights counted more). Must be the same length as
            filepaths. Must be non-negative.

    Returns:
        A generator of PlaylistTrack objects, in order of distance. Can be consumed
        until needs are exhausted.
    """
    # get weights
    if weights is not None:
        # validate weights
        if len(weights) != len(filepaths):
            raise ValueError("weights must be the same length as filepaths.")
        elif any(w < 0 for w in weights):
            raise ValueError("Weights must be non-negative.")
        elif sum(weights) == 0:
            raise ValueError("Weights must sum to a non-zero value.")
    else:
        weights = [1] * len(filepaths)

    # upload files and weights to a temporary table to avoid a huge where clause
    tmp_name = make_temp_table(
        session=session,
        types={"filepath": "text", "weight": "float"},
        data=[
            {"filepath": str(fp), "weight": w}
            for fp, w in zip(filepaths, weights)
            if w > 0
        ],
        pk="filepath",
    )

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        with base as (
            select filepath, tmp_.weight, local_files.embedding
            from {schema}.local_files
            inner join {tmp_name} as tmp_ using (filepath)
            where local_files.embedding_success
              and local_files.embedding_duration_seconds >= 60
        )

        , distances as (
            select
                local_files.filepath as filepath
                , case 
                    when sum(base.weight) > 0  -- in case filtered to only 0 weights
                    then (
                        sum((base.embedding <-> local_files.embedding) * base.weight)
                        / sum(base.weight)
                    )
                  end  as distance

            from {schema}.local_files
            cross join base

            where true
                and local_files.embedding_success
                and local_files.embedding_duration_seconds >= 60
                and not local_files.filepath = base.filepath
                and local_files.artist_mbid is not null

            group by local_files.filepath
            having sum(base.weight) > 0
        )

        select
            filepath
            , f.recording_mbid
            , f.release_mbid
            , f.release_group_mbid
            , f.artist_mbid
            , coalesce(f.album_artist_mbid, f.artist_mbid) as album_artist_mbid
            , d.distance

        from distances as d
        inner join {schema}.local_files as f using (filepath)
        where f.artist_mbid != any(:mashup_artists)
          and coalesce(f.album_artist_mbid, f.artist_mbid) != any(:mashup_artists)
          and f.recording_mbid != any(:earworms)
        order by d.distance asc
        limit :limit
    """
    res = session.execute(
        text(sql),
        params={
            "filepaths": list(set(map(str, filepaths))),
            "limit": limit,
            "mashup_artists": list(MASHUP_ARTISTS),
            "earworms": list(EARWORMS),
        },
        execution_options=dict(yield_per=1, stream_results=True, max_row_buffer=1),
    )

    for (
        filepath,
        recording_mbid,
        release_mbid,
        release_group_mbid,
        artist_mbid,
        album_artist_mbid,
        distance,
    ) in res:
        yield Track(
            filepath=Path(filepath),
            recording_mbid=recording_mbid,
            release_mbid=release_mbid,
            release_group_mbid=release_group_mbid,
            artist_mbid=artist_mbid,
            album_artist_mbid=album_artist_mbid,
            distance=distance,
        )


def get_most_similar_tracks(
    filepaths: list[Path],
    session: Session,
    limit: int = 20,
    limit_per_artist: Optional[int] = None,
    weights: Optional[list[float]] = None,
) -> list[Track]:
    """Get a listing of similar songs.

    Performs logic to limit the number of songs per artist, while still returning the
    most similar songs.

    Args:
        filepaths: List of filepaths. Passed to stream_similar_tracks.
        session: sqlalchemy session to use.
        limit: Number of songs to include in the playlist.
        limit_per_artist: Maximum number of songs per artist.
        weights: List of weights for each filepath. If provided, the aggregate distance
            will be weighted (larger weights counted more). Must be the same length as
            filepaths. Must be non-negative.

    Returns:
        A list of filepaths, sorted by distance.
    """
    if not filepaths:
        raise ValueError("No filepaths provided.")

    # if not limit_per_artist, then we don't need to do any extra logic
    if not limit_per_artist:
        return [
            i
            for i in stream_similar_tracks(
                filepaths=filepaths, session=session, limit=limit, weights=weights
            )
        ]

    # else, consume the generator and limit the number of songs per artist
    tracks, artist_counts = [], Counter()
    for track in stream_similar_tracks(filepaths=filepaths, session=session):
        if any(
            getattr(track, attr) is None
            for attr in ["artist_mbid", "album_artist_mbid"]
        ):
            continue

        track_artists = list(set([track.artist_mbid, track.album_artist_mbid]))
        track_artist_counts = [artist_counts[mbid] for mbid in track_artists]

        if all(c < limit_per_artist for c in track_artist_counts):
            tracks.append(track)
            for mbid in track_artists:
                if mbid not in SPECIAL_PURPOSE_ARTISTS:
                    artist_counts[mbid] += 1

        if len(tracks) >= limit:
            break

    return tracks
