"""Playlist generation utilities for user provided files."""
import random
from pathlib import Path
from typing import List, Optional

import click

from .. import utils_

SQL_TEMPLATE = """
with request as (
    {request_sql}
)

, base as (
    select filepath, embedding, artist_mbid
    from {schema}.local_files_flat
    inner join request using (filepath)
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
      and local_files_flat.filepath not in (select filepath from request)

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
where artist_rank <= 2
order by distance
limit {limit}
"""


def get_playlist(
    request_sql: str,
    schema: str,
    base_dir: Path,
    limit: int = 20,
    params: Optional[dict] = None,
    shuffle: bool = True,
    seed_files: Optional[List[Path]] = None,
) -> List[Path]:
    """Get a playlist of similar songs.

    Users must provide a SQL query that returns a single column of filepaths.
    This will be joined to the local_files_flat table to get the embedding distance
    between the requested song and all other songs in the database.

    Args:
        request_sql: SQL query that returns a single column of filepaths.
        schema: moomoo dbt schema.
        limit: Number of songs to include in the playlist.
        params: Parameters to pass to the SQL query.
        shuffle: Shuffle the playlist or not.
        seed_files: FIles which will be included at the start of the playlist.
        **kwargs: Additional arguments to pass to the xspf.Playlist constructor.

    Returns:
        List of Path objects, local to the database. As such, they must be resolved
        to the system path before they can be used.
    """
    sql = SQL_TEMPLATE.format(request_sql=request_sql, limit=limit, schema=schema)
    seed_files = seed_files or []
    tracks = [
        base_dir / row["filepath"]
        for row in utils_.execute_sql_fetchall(sql, params)
        if (base_dir / row["filepath"]) not in seed_files
    ]

    if shuffle:
        random.shuffle(tracks)

    tracks = seed_files + tracks
    if not all([t.exists() for t in tracks]):
        raise ValueError("ERROR: Some tracks in the playlist do not exist.")

    return tracks


@click.command("from-file")
@click.argument(
    "filepath",
    type=click.Path(exists=True, dir_okay=True, path_type=Path),
    required=True,
)
@click.option(
    "--schema",
    required=True,
    help="Moomoo dbt schema, used to obtain embeddings and metadata.",
    type=str,
    envvar="MOOMOO_DBT_SCHEMA",
)
@click.option(
    "-n",
    "--count",
    default=20,
    help="Number of songs to include in the playlist.",
    type=int,
)
@click.option(
    "--shuffle/--no-shuffle",
    default=True,
    help="Shuffle the playlist or not.",
    is_flag=True,
)
@click.option(
    "-o",
    "--out",
    help=(
        "Choice of output format. "
        + "`strawberry` loads directly into the strawberry player. "
        + "`xspf` (default) prints the playlist to stdout in xspf format."
    ),
    type=click.Choice(["stdout", "strawberry"]),
    default="stdout",
)
def cli(filepath: Path, schema: str, count: int, shuffle: bool, out: str):
    """Create a playlist from a file."""
    base_dir, local_paths = utils_.resolve_db_path(filepath, schema)

    # filter to files with valid embedding
    request_sql = f"""
    select filepath
    from {schema}.local_files_flat
    where filepath = any(%(local_paths)s)
      and embedding_success
    """
    res = utils_.execute_sql_fetchall(
        request_sql, {"local_paths": list(map(str, local_paths))}
    )
    local_paths = [Path(row["filepath"]) for row in res]

    if not local_paths:
        raise ValueError(f"{filepath} has no embeddings in the database.")

    # get playlist
    playlist = get_playlist(
        request_sql=request_sql,
        schema=schema,
        limit=count,
        base_dir=base_dir,
        params={"local_paths": list(map(str, local_paths))},
        shuffle=shuffle,
        seed_files=[base_dir / random.choice(local_paths)],
    )

    # render
    utils_.render_playlist(
        playlist,
        out,
        annotation=f"moomoo generated from-file: {filepath}",
    )
