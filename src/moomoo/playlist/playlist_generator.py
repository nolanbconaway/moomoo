"""Playlist generation utilities for user provided files."""
import random
from functools import cache
from pathlib import Path
from typing import Optional

from psycopg import Connection

from .. import utils_

# maximum number of source paths to use when generating a playlist. this is to avoid
# doing a huge number of pairwise distance calculations in the database.
MAX_SOURCE_PATHS = 25

SQL_TEMPLATE = """
with base as (
    select filepath, embedding, artist_mbid
    from {schema}.local_files_flat
    where filepath = any(%(filepaths)s)
)

, distances as (
    select
        local_files_flat.filepath as filepath
        , avg(base.embedding <-> local_files_flat.embedding) as distance

    from base
    cross join {schema}.local_files_flat

    where local_files_flat.embedding_success
      and local_files_flat.embedding_duration_seconds >= 60
      and local_files_flat.artist_mbid is not null
      and not local_files_flat.filepath = any(%(filepaths)s)

    group by local_files_flat.filepath
)

, ranked as (
    select
        local_files_flat.filepath
        , distances.distance
        , row_number() over (
            partition by local_files_flat.artist_mbid order by distance
        ) as artist_rank

    from distances
    inner join {schema}.local_files_flat using (filepath)
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
    This will be joined to the local_files_flat table to get the embedding distance
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
    def from_files(cls, files: list[Path], schema: str) -> "PlaylistGenerator":
        """Create a playlist generator from a list of files."""
        request_sql = f"""
            select filepath
            from {schema}.local_files_flat
            where filepath = any(%(filepaths)s)
        """
        return cls(request_sql, sql_params={"filepaths": [str(f) for f in files]})

    @classmethod
    def from_parent_path(cls, path: Path, schema: str) -> "PlaylistGenerator":
        """Create a playlist generator from a parent path."""
        request_sql = f"""
            select filepath
            from {schema}.local_files_flat
            where filepath like %(path)s
            order by random()
            limit 25
        """
        return cls(request_sql, sql_params={"path": f"{path}%"})

    @cache
    def list_requested_paths(self) -> list[Path]:
        """List the paths requested by the user.

        Cached, btw.
        """
        return [
            Path(row["filepath"])
            for row in utils_.execute_sql_fetchall(self.request_sql, self.sql_params)
        ]

    def get_playlist(
        self,
        schema: str,
        limit: int = 20,
        limit_per_artist: int = 2,
        shuffle: bool = True,
        seed_count: int = 0,
        conn: Optional[Connection] = None,
    ) -> utils_.PlaylistResult:
        """Get a playlist of similar songs.

        Args:
            schema: moomoo dbt schema.
            limit: Number of songs to include in the playlist.
            shuffle: Shuffle the playlist or not.
            seed_files: Files which will be included at the start of the playlist.
            limit_per_artist: Maximum number of songs per artist.
            seed_count: Number of seed files from the request to include at the start of
                the playlist.

        Returns:
            List of Path objects, local to the database. As such, they must be resolved
            to system paths on the client side.
        """
        # TODO: support existing connections
        sql = SQL_TEMPLATE.format(
            limit=limit,
            schema=schema,
            limit_per_artist=limit_per_artist,
        )

        filepaths = sorted(self.list_requested_paths())
        if not filepaths:
            raise NoFilesRequestedError("No paths requested (or found via request).")
        elif len(filepaths) > MAX_SOURCE_PATHS:
            filepaths = sorted(random.sample(filepaths, MAX_SOURCE_PATHS))

        seed_files = [] if seed_count == 0 else random.sample(filepaths, seed_count)
        tracks = [
            Path(row["filepath"])
            for row in utils_.execute_sql_fetchall(
                sql, params={"filepaths": list(map(str, filepaths))}, conn=conn
            )
        ]

        if shuffle:
            random.shuffle(tracks)

        return utils_.PlaylistResult(
            playlist=seed_files + tracks, source_paths=filepaths
        )
