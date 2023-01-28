"""Utility functions for the good of all."""
import datetime
import os
import time
from pathlib import Path
from typing import List

import musicbrainzngs
import psycopg2
import psycopg2.extras

# I think musicbrainzngs falls under the 50 requests per second allowed per:
# https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting#User-Agent
#
# This is generous at 10/s.
SLEEP_S = 0.1


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


def pg_connect(
    dsn: str = None, dict_cursor: bool = False
) -> psycopg2.extensions.connection:
    """Connect to the db.

    Option to use dict cursors instead of regular cursors via the dict_cursor kwarg.
    """
    if dict_cursor:
        kw = dict(cursor_factory=psycopg2.extras.DictCursor)
    else:
        kw = dict()
    return psycopg2.connect(dsn or os.environ["POSTGRES_DSN"], **kw)


def execute_sql_fetchall(sql: str, params: dict = None, dsn: str = None) -> List[dict]:
    """Execute a SQL statement and return all results via dict cursors."""
    with pg_connect(dsn=dsn, dict_cursor=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or dict())
            return cur.fetchall()


def create_table(schema: str, table: str, ddl: List[str]):
    """Create a table in the db with multiple DDL statements.

    Useful for creating tables with indexes. Drops the table if it already exists.
    """
    print('Creating table "{}" in schema "{}"...'.format(table, schema))
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
    return datetime.datetime.fromisoformat(iso_date).replace(
        tzinfo=datetime.timezone.utc
    )


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )


def set_mbz_client():
    """Set the musicbrainzngs client."""
    musicbrainzngs.set_useragent(
        app="moomoo", version=moomoo_version(), contact=os.environ["CONTACT_EMAIL"]
    )


def get_recording_data(recording_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    set_mbz_client()
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


def get_release_data(release_mbid: str) -> dict:
    """Get release data from MusicBrainz."""
    set_mbz_client()
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


def get_artist_data(artist_mbid: str) -> dict:
    """Get artist data from MusicBrainz."""
    set_mbz_client()
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
    """Enrich a MusicBrainz ID with data from MusicBrainz.

    This is the main entry point intended, as it comes with rate limiting, error
    handling, etc. I have found speed at the rate of 1 req/s in practice.
    """
    fn = {
        "recording": get_recording_data,
        "release": get_release_data,
        "artist": get_artist_data,
    }.get(entity)

    if fn is None:
        raise ValueError(
            "Invalid entity type. Require: {'recording', 'release', 'artist'}"
        )

    try:
        return dict(_success=True, data=fn(mbid))
    except musicbrainzngs.musicbrainz.MusicBrainzError as e:
        return dict(_success=False, error=str(e))
    finally:
        time.sleep(SLEEP_S)
