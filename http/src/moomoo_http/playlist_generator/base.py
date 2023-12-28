"""Base utilties for playlist generation."""
import abc
import os
import random
from collections import Counter
from dataclasses import dataclass
from operator import attrgetter
from pathlib import Path
from typing import Generator, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass(frozen=True)
class Track:
    """A candidate track in a playlist.

    Contains metadata about the track that can be used to construct a playlist.
    """

    filepath: Path
    artist_mbid: Optional[UUID] = None
    album_artist_mbid: Optional[UUID] = None
    distance: Optional[float] = None

    def any_missing_info(self) -> bool:
        """Check if any of the metadata is missing."""
        return (
            self.artist_mbid is None
            or self.album_artist_mbid is None
            or self.distance is None
        )


@dataclass(frozen=True)
class Playlist:
    """A full playlist.

    This object should contain all that is needed to populate client-side playlist.
    """

    playlist: list[Track]
    description: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to a dictionary, appropriate for json serialization."""
        return {
            "playlist": list(map(str, map(attrgetter("filepath"), self.playlist))),
            "description": self.description,
        }

    def shuffle(self) -> "Playlist":
        """Shuffle the playlist inplace."""
        random.shuffle(self.playlist)
        return self


class NoFilesRequestedError(Exception):
    """No files requested by the user."""

    pass


class BasePlaylistGenerator(abc.ABC):
    """Base class for playlist generators.

    Subclasses should implement get_playlist. Mostly here for type hinting with mixed
    types of playlist generators.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Name of the playlist generator."""
        ...

    @abc.abstractmethod
    def get_playlist(
        self,
        session: Session,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
    ) -> Playlist:
        """Get a playlist.

        Args:
            session: Sqlalchemy session.
            limit: Number of songs to include in the playlist.
            limit_per_artist: Maximum number of songs per artist.
            shuffle: Whether to shuffle the playlist.
            seed_count: Number of songs to seed the playlist with.

        Returns:
            A Playlist object.
        """
        ...


def stream_similar_tracks(
    filepaths: list[Path], session: Session, limit: Optional[int] = 500
) -> Generator[Track, None, None]:
    """Stream similar tracks to filepaths.

    This is best used internally, as it does not return a list of filepaths, but rather
    a generator of PlaylistTrack objects. Consumers will need to iterate over the
    generator to and implement logic to limit the number of songs per artist, etc.

    Args:
        filepaths: List of paths files.
        session: Sqlalchemy session.
        limit: Maximum number of tracks to return (default 500).

    Returns:
        A generator of PlaylistTrack objects, in order of distance. Can be consumed
        until needs are exhausted.
    """

    schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        with base as (
            select filepath, embedding
            from {schema}.local_files where filepath = any(:filepaths)
        )

        , distances as (
            select
                local_files.filepath as filepath
                , avg((base.embedding <-> local_files.embedding)) as distance

            from base
            cross join {schema}.local_files

            where local_files.embedding_success
            and local_files.embedding_duration_seconds >= 60
            and not local_files.filepath = any(:filepaths)
            and local_files.artist_mbid is not null

            group by local_files.filepath
        )

        select
            filepath
            , f.artist_mbid
            , coalesce(f.album_artist_mbid, f.artist_mbid) as album_artist_mbid
            , d.distance

        from distances as d
        inner join {schema}.local_files as f using (filepath)
        order by d.distance asc
        limit :limit
    """

    # start by expanding the parallel workers???
    # session.execute(text("set local max_parallel_workers_per_gather = 12"))
    # session.execute(text("set local max_parallel_workers = 12"))
    # session.execute(text("set local hnsw.ef_search = 10"))
    res = session.execute(
        text(sql),
        params={"filepaths": list(map(str, filepaths)), "limit": limit},
        execution_options=dict(yield_per=1, stream_results=True, max_row_buffer=1),
    )

    for filepath, artist_mbid, album_artist_mbid, distance in res:
        yield Track(
            filepath=Path(filepath),
            artist_mbid=artist_mbid,
            album_artist_mbid=album_artist_mbid,
            distance=float(distance),
        )


def get_most_similar_tracks(
    filepaths: list[Path],
    session: Session,
    limit: int = 20,
    limit_per_artist: Optional[int] = None,
) -> list[Track]:
    """Get a listing of similar songs.

    Performs logic to limit the number of songs per artist, while still returning the
    most similar songs.

    Args:
        filepaths: List of filepaths. Passed to stream_similar_tracks.
        session: sqlalchemy session to use.
        limit: Number of songs to include in the playlist.
        limit_per_artist: Maximum number of songs per artist.

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
                filepaths=filepaths, session=session, limit=limit
            )
        ]

    # else, consume the generator and limit the number of songs per artist
    tracks, artist_counts = [], Counter()
    for track in stream_similar_tracks(
        filepaths=filepaths, session=session, limit=limit * limit_per_artist + 1
    ):
        if track.any_missing_info():
            continue

        if (
            artist_counts[track.artist_mbid] < limit_per_artist
            and artist_counts[track.album_artist_mbid] < limit_per_artist
        ):
            tracks.append(track)
            artist_counts[track.artist_mbid] += 1

            if track.album_artist_mbid != track.artist_mbid:
                artist_counts[track.album_artist_mbid] += 1

        if len(tracks) >= limit:
            break

    return tracks
