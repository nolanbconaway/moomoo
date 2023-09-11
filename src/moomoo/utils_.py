"""Utility functions for the good of all.

Put no specialty imports beyond cli, postgres here, as the thin client needs this.
"""
import datetime
import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import click
import psycopg
import xspf_lib as xspf
from pgvector.psycopg import register_vector
from psycopg.rows import dict_row


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder for UUIDs."""

    def default(self, obj):
        """Encode UUIDs as hex strings."""
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


def _pg_connect(*args, **kwargs) -> psycopg.Connection:
    """Connect to the db; for mocking purposes."""
    return psycopg.connect(*args, **kwargs)


def pg_connect() -> psycopg.Connection:
    """Connect to the db."""
    conn = _pg_connect(os.environ["POSTGRES_DSN"])
    register_vector(conn)
    return conn


def execute_sql_fetchall(
    sql: str, params: dict = None, conn: psycopg.Connection = None
) -> list[dict]:
    """Execute a SQL statement and return all results via dict cursors."""

    def f(c: psycopg.Connection):
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or dict())
            return cur.fetchall()

    if conn is None:
        with pg_connect() as conn:
            return f(conn)

    return f(conn)


def create_table(schema: str, table: str, ddl: list[str]):
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


@dataclass
class PlaylistResult:
    """A playlist result.

    Contains the target paths and the local paths used to generate it. Has methods
    to render the playlist in different formats.
    """

    playlist: list[Path]
    source_paths: list[Path]

    def to_xspf(self) -> xspf.Playlist:
        """Convert to an xspf playlist."""
        return xspf.Playlist(
            trackList=[xspf.Track(location=str(p)) for p in self.playlist],
            creator="moomoo",
            annotation=f"Generated via {len(self.source_paths)} source path(s).",
        )

    def to_json(self) -> str:
        """Convert to a json string."""
        return json.dumps(
            dict(
                playlist=[str(p) for p in self.playlist],
                source_paths=[str(p) for p in self.source_paths],
            )
        )

    def to_xml(self):
        """Convert to an xspf xml string."""
        return self.to_xspf().xml_string()

    def to_strawberry(self, wait_seconds: float = 0.5):
        """Load the playlist into strawberry."""
        with tempfile.NamedTemporaryFile() as f:
            fp = Path(f.name)
            fp.write_text(self.to_xml())
            subprocess.run(["strawberry", "--load", f.name])
            time.sleep(wait_seconds)

    def render(self, method: str):
        """Render the playlist."""
        if method not in ["json", "xml", "strawberry"]:
            raise ValueError(f"Unknown method {method}")

        if method == "json":
            click.echo(self.to_json())
        elif method == "xml":
            click.echo(self.to_xml())
        elif method == "strawberry":
            self.to_strawberry()
