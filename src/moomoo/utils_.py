"""Utility functions for the good of all."""
import datetime
import os
from pathlib import Path
from typing import Iterable, Iterator, List

import click
import musicbrainzngs
import psycopg
from pgvector.psycopg import register_vector
from psycopg.rows import dict_row


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


# set user agent for all musicbrainzngs requests
musicbrainzngs.set_useragent(
    app="moomoo", version=moomoo_version(), contact=os.environ["CONTACT_EMAIL"]
)


def _pg_connect(*args, **kwargs) -> psycopg.Connection:
    """Connect to the db; for mocking purposes."""
    return psycopg.connect(*args, **kwargs)


def pg_connect(dsn: str = None) -> psycopg.Connection:
    """Connect to the db."""
    conn = _pg_connect(dsn or os.environ["POSTGRES_DSN"])
    register_vector(conn)
    return conn


def execute_sql_fetchall(sql: str, params: dict = None, dsn: str = None) -> List[dict]:
    """Execute a SQL statement and return all results via dict cursors."""
    with pg_connect(dsn=dsn) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or dict())
            return cur.fetchall()


def create_table(schema: str, table: str, ddl: List[str]):
    """Create a table in the db with multiple DDL statements.

    Useful for creating tables with indexes. Drops the table if it already exists.

    TODO: make dropping the table a kwarg. create if not exists.
    """
    click.echo('Creating table "{}" in schema "{}"...'.format(table, schema))
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"drop table if exists {schema}.{table}".format(
                    schema=schema, table=table
                )
            )
            for sql in ddl:
                cur.execute(sql.format(schema=schema, table=table))
        conn.commit()


def check_table_exists(schema: str, table: str) -> bool:
    """Check if a table exists in the db."""
    with pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select 1
                from information_schema.tables 
                where table_schema = %(schema)s
                and table_name = %(table)s
                limit 1
                """,
                dict(schema=schema, table=table),
            )
            res = cur.fetchall()
    return any(res)


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY-MM-DD date string to UTC datetime."""
    dt = datetime.datetime.fromisoformat(iso_date)
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=datetime.timezone.utc)


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )


def _get_recording_data(recording_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    return musicbrainzngs.get_recording_by_id(
        recording_mbid,
        includes=[
            "artists",
            "releases",
            "artist-credits",
            "aliases",
            "tags",
            "ratings",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
        ],
    )


def _get_release_data(release_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    return musicbrainzngs.get_release_by_id(
        release_mbid,
        includes=[
            "artists",
            "labels",
            "recordings",
            "aliases",
            "tags",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
        ],
    )


def _get_artist_data(artist_mbid: str) -> dict:
    """Get artist data from MusicBrainz."""
    return musicbrainzngs.get_artist_by_id(
        artist_mbid,
        includes=[
            "releases",
            "various-artists",
            "aliases",
            "area-rels",
            "artist-rels",
            "label-rels",
            "place-rels",
            "url-rels",
            "tags",
            "ratings",
        ],
    )


def annotate_mbid(mbid: str, entity: str) -> dict:
    """Enrich a MusicBrainz IDs with data from MusicBrainz.

    Expected input:

    - mbid: the MusicBrainz ID
    - entity: the type of entity, e.g. 'recording', 'release', 'artist'

    Returns a dicts with the following keys:

    - _success: boolean indicating whether the request was successful
    - _args: a dict containing the mbid and entity type of the request
    - error: error message if the request was not successful
    - data: the data returned from MusicBrainz if the request was successful
    """
    args = dict(mbid=mbid, entity=entity)
    fn = {
        "recording": _get_recording_data,
        "release": _get_release_data,
        "artist": _get_artist_data,
    }.get(entity)

    if fn is None:
        return dict(_success=False, _args=args, error=f"Unknown entity type: {entity}.")

    try:
        return dict(_success=True, _args=args, data=fn(mbid))
    except Exception as e:
        return dict(_success=False, _args=args, error=str(e))


def annotate_mbid_batch(mbids_maps: Iterable[dict]) -> Iterator[dict]:
    """Enrich MusicBrainz IDs with data from MusicBrainz.

    Expected input is a list/iterable of dicts with the following keys:

    - mbid: the MusicBrainz ID
    - entity: the type of entity, e.g. 'recording', 'release', 'artist'

    Yields a generator of dicts with the following keys:

    - _success: boolean indicating whether the request was successful
    - _args: a dict containing the mbid and entity type of the request
    - error: error message if the request was not successful
    - data: the data returned from MusicBrainz if the request was successful
    """
    for mbid_map in mbids_maps:
        mbid = mbid_map["mbid"]
        entity = mbid_map["entity"]
        yield annotate_mbid(mbid, entity)
