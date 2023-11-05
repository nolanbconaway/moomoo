"""Playlist generation utilities for user provided files."""
import os
import random
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from .db import execute_sql_fetchall

# maximum number of source paths to use when generating a playlist. this is to avoid
# doing a huge number of pairwise distance calculations in the database.
MAX_SOURCE_PATHS = 25


SQL_TEMPLATE = """
with base as (
    select filepath, embedding, artist_mbid
    from {schema}.local_files
    where filepath = any(:filepaths)
)

, distances as (
    select
        local_files.filepath as filepath
        , avg(base.embedding <-> local_files.embedding) as distance

    from base
    cross join {schema}.local_files

    where local_files.embedding_success
      and local_files.embedding_duration_seconds >= 60
      and local_files.artist_mbid is not null
      and not local_files.filepath = any(:filepaths)

    group by local_files.filepath
)

, ranked as (
    select
        local_files.filepath
        , distances.distance
        , row_number() over (
            partition by local_files.artist_mbid order by distance
        ) as artist_rank

    from distances
    inner join {schema}.local_files using (filepath)
)

select filepath, distance
from ranked
where artist_rank <= {limit_per_artist}
order by distance, filepath
limit {limit}
"""


class NoFilesRequestedError(Exception):
    """No files requested by the user."""

    pass


class PlaylistGenerator:
    """A playlist generator, wrapping simple database access.

    Users must provide a SQL query that returns a single column of filepaths.
    This will be joined to the local_files table to get the embedding distance
    between the requested song and all other songs in the database.

    Users may also supply additional parameters to the SQL query, which will be
    passed to the database when executing the request_sql.

    This class easiest created with helper from_* methods, which template out the SQL.
    """

    def __init__(self, request_sql: str, sql_params: Optional[dict] = None):
        """Init the playlist generator."""
        # check that request sql returns a single column of filepaths. basic string
        # grepping here
        if "filepath" not in request_sql or "select" not in request_sql:
            raise ValueError(
                "Request SQL must return a single column of filepaths named 'filepath'."
            )

        self.request_sql = request_sql
        self.sql_params = sql_params

    @classmethod
    def from_files(cls, files: list[Path]) -> "PlaylistGenerator":
        """Create a playlist generator from a list of files."""
        files_ = list(set(files))  # dedupe

        # if only one path, it makes sense to use the parent path generator instead
        if len(files_) == 1:
            return cls.from_parent_path(files[0])
        elif len(files_) > MAX_SOURCE_PATHS:
            files_ = random.sample(files_, MAX_SOURCE_PATHS)

        request_sql = f"""
            select filepath
            from {os.environ["MOOMOO_DBT_SCHEMA"]}.local_files
            where filepath = any(:filepaths)
        """

        return cls(request_sql, sql_params={"filepaths": [str(f) for f in files_]})

    @classmethod
    def from_parent_path(cls, path: Path) -> "PlaylistGenerator":
        """Create a playlist generator from a parent path."""
        request_sql = f"""
            select filepath
            from {os.environ["MOOMOO_DBT_SCHEMA"]}.local_files
            where filepath like :path
            order by random()
            limit 25
        """
        return cls(request_sql, sql_params={"path": f"{path}%"})

    def list_requested_paths(self, session: Session) -> list[Path]:
        """List the paths requested by the user."""
        return [
            Path(row["filepath"])
            for row in execute_sql_fetchall(
                session=session, sql=self.request_sql, params=self.sql_params
            )
        ]

    def get_playlist(
        self,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
        session: Session = None,
    ) -> tuple[list[Path], list[Path]]:
        """Get a playlist of similar songs.

        Args:
            schema: moomoo dbt schema.
            limit: Number of songs to include in the playlist.
            shuffle: Shuffle the playlist or not.
            seed_files: Files which will be included at the start of the playlist.
            limit_per_artist: Maximum number of songs per artist.
            seed_count: Number of seed files from the request to include at the start of
                the playlist.
            session: Optional sqlalchemy session to use.

        Returns:
            A tuple of (playlist, source_paths).
        """
        sql = SQL_TEMPLATE.format(
            limit=limit,
            schema=os.environ["MOOMOO_DBT_SCHEMA"],
            limit_per_artist=limit_per_artist,
        )

        filepaths = sorted(self.list_requested_paths(session=session))
        if not filepaths:
            raise NoFilesRequestedError("No paths requested (or found via request).")
        elif len(filepaths) > MAX_SOURCE_PATHS:
            filepaths = sorted(random.sample(filepaths, MAX_SOURCE_PATHS))

        seed_files = [] if seed_count == 0 else random.sample(filepaths, seed_count)
        tracks = [
            Path(row["filepath"])
            for row in execute_sql_fetchall(
                sql=sql,
                params={"filepaths": list(map(str, filepaths))},
                session=session,
            )
        ]

        if shuffle:
            random.shuffle(tracks)

        return seed_files + tracks, filepaths
