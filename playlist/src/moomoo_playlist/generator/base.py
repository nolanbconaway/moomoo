"""Base utilties for playlist generation."""

import abc
import os
from collections import Counter
from collections.abc import Generator
from itertools import islice
from math import log
from pathlib import Path
from typing import Optional
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

# grabbed this via median score across all artists. used as default value in case we have no artist
# similarity data.
BASELINE_CF_SCORE = 0.2475996481320529  # ~1.281


# i looked at the most similiar tracks and found that up until this point, the tracks were more or
# less the same (sometimes different artists, but are silent tracks, etc).
#
# This will only exclude the ~700 most similar pairs.
MINIMUM_COSINE_SIMILARITY = 0.5


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
            x: Listen count.

        Returns:
            A weight for use in similarity calculations.
        """
        return log(2 + max(x, 0))

    @staticmethod
    def recency_score_to_weight(x: int, fac: float = 1.0) -> float:
        """Convert a recency score to a weight.

        Recency is defined as a >0 score, with higher scores indicating more recent listens.
        We wish to make a weight multiplier which makes streaming recent tracks less likely, which
        means we need to covert the recency score to a weight < 1.

        This is done via transformation 1 / (1 + x * fac), which will return a weight between 0-1,
        with higher recency scores resulting in lower weights.

        Args:
            x: Listen count.
            fac: Factor to multiply the recency score by.

        Returns:
            A weight for use in similarity calculations.
        """
        return 1 / (1 + x * fac)


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
        for row in execute_sql_fetchall(sql=sql, params={"username": username}, session=session)
    }

    # add any missing filepaths with 0 listens
    return {fp: rows.get(fp, 0) for fp in filepaths}


def fetch_recently_played_tracks(
    username: str, session: Session, limit: int = 1000
) -> dict[Path, float]:
    """Fetch the most recently played tracks for a user, along with the recency score.

    Args:
        username: Username to fetch listen counts for.
        session: Sqlalchemy session.
        limit: Number of tracks to fetch.

    Returns:
        A list of filepaths, ordered by most recently played.
    """
    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select filepath, recency_score
        from {schema}.file_listen_counts
        where username = :username
          and last30_listen_count > 0
          and recency_score is not null
        order by recency_score desc
        limit :limit
    """
    return {
        Path(row["filepath"]): float(row["recency_score"])
        for row in execute_sql_fetchall(
            sql=sql, params={"username": username, "limit": limit}, session=session
        )
    }


@db_retry
def stream_similar_tracks(
    filepaths: list[Path],
    session: Session,
    limit: Optional[int] = 2000,
    weights: Optional[list[float]] = None,
    predicate_weights: dict[Path, float] | None = None,
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
        predicate_weights: Dictionary of filepaths to weights. If provided, the distance between the
            potentially streamed filepaths will be _divided_ by the weight. This is useful for
            boosting/reducing the likelihood of certain tracks being included in the stream. Larger
            weights will then decrease the distance between the tracks, so they are more likely to
            be included in the stream.

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

    # check predicate weights
    if any(w < 0 for w in (predicate_weights or dict()).values()):
        raise ValueError("Predicate weights must be non-negative.")

    # upload weights to temporary tables for later joining against the local_files table
    base_weights_table = make_temp_table(
        session=session,
        types={"filepath": "text", "weight": "float"},
        data=[{"filepath": str(fp), "weight": w} for fp, w in zip(filepaths, weights) if w > 0],
        pk="filepath",
    )

    predicate_weights_table = make_temp_table(
        session=session,
        types={"filepath": "text", "weight": "float"},
        data=[
            {"filepath": str(fp), "weight": w} for fp, w in (predicate_weights or dict()).items()
        ],
        pk="filepath",
    )

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        with base as (
            select filepath, artist_mbid, tmp_.weight, local_files.embedding
            from {schema}.local_files
            inner join {base_weights_table} as tmp_ using (filepath)
            where local_files.embedding_success
              and local_files.embedding_duration_seconds >= 60
        )

        , distances as (
            select
                local_files.filepath as filepath
                , case 
                    when sum(base.weight) > 0  -- in case filtered to only 0 weights
                    then (
                        sum(
                            (base.embedding <-> local_files.embedding)
                            * base.weight
                            / coalesce(exp(cf_scores.score_value - {BASELINE_CF_SCORE}), 1)
                        )
                        / sum(base.weight)
                    )
                  end as distance

            from {schema}.local_files
            cross join base
            left join {schema}.listenbrainz_collaborative_filtering_scores as cf_scores
                on base.artist_mbid = cf_scores.artist_mbid_a
                and local_files.artist_mbid = cf_scores.artist_mbid_b

            where true
                and local_files.embedding_success
                and local_files.embedding_duration_seconds >= 60
                and local_files.filepath not in (select filepath from {base_weights_table})
                and local_files.artist_mbid is not null

                and (base.embedding <-> local_files.embedding) > {MINIMUM_COSINE_SIMILARITY}

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
            , d.distance / coalesce(weights.weight, 1.0) as distance

        from distances as d
        left join {predicate_weights_table} as weights using (filepath)
        inner join {schema}.local_files as f using (filepath)
        where coalesce(weights.weight, 1.0) > 0
        order by d.distance / coalesce(weights.weight, 1.0) asc, md5(filepath)
        limit :limit
    """
    res = session.execute(
        text(sql),
        params={"limit": limit},
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
    predicate_weights: dict[Path, float] | None = None,
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
        predicate_weights: Dictionary of filepaths to weights. If provided, the distance between the
            potentially streamed filepaths will be _divided_ by the weight. This is useful for
            boosting/reducing the likelihood of certain tracks being included in the stream. Larger
            weights will then decrease the distance between the tracks, so they are more likely to
            be included in the stream.

    Returns:
        A list of filepaths, sorted by distance.
    """
    if not filepaths:
        raise ValueError("No filepaths provided.")

    def stream() -> Generator[Track, None, None]:
        yield from stream_similar_tracks(
            filepaths=filepaths,
            session=session,
            weights=weights,
            predicate_weights=predicate_weights,
        )

    # if not limit_per_artist, then we don't need to do any extra logic. just send the limit to the
    # stream
    if not limit_per_artist:
        return list(islice(stream(), limit))

    # else, consume the generator up to the total limit and limit the number of songs per artist
    tracks, artist_counts = [], Counter()
    for track in stream():
        # the query should protect against returning the provided filepaths, but just in case
        if track.filepath in filepaths:
            continue

        # skip tracks with missing artist mbids
        if any(getattr(track, attr) is None for attr in ["artist_mbid", "album_artist_mbid"]):
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
