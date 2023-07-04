"""Utility functions for the good of all."""
import datetime
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple

import click
import musicbrainzngs
import psycopg
import xspf_lib as xspf
from pgvector.psycopg import register_vector
from psycopg.rows import dict_row


def moomoo_version() -> str:
    """Get the current moomoo version."""
    return (Path(__file__).resolve().parent / "version").read_text().strip()


# set user agent for all musicbrainzngs requests
musicbrainzngs.set_useragent(
    app="moomoo", version=moomoo_version(), contact=os.environ.get("CONTACT_EMAIL")
)


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
) -> List[dict]:
    """Execute a SQL statement and return all results via dict cursors."""

    def f(c: psycopg.Connection):
        with c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or dict())
            return cur.fetchall()

    if conn is None:
        with pg_connect() as conn:
            return f(conn)

    return f(conn)


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
    # check contact email set
    if not os.environ.get("CONTACT_EMAIL"):
        raise ValueError("CONTACT_EMAIL environment variable not set.")

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


def resolve_db_path(syspath: Path, schema: str) -> Tuple[Path, List[Path]]:
    """Resolve a system filepath to find the local path used in the db.

    Uses the local_files_flat table in the dbt schema to find the local path, based on
    the db filepath matching the end of the system path.

    args:
        filepath: the filepath to resolve
        schema: the schema to use. should be the analytic DBT schema, as we use
            the local_files_flat table.

    Returns a tuple of the base path and a list of local paths known to the database.
    """
    syspath = syspath.resolve()
    if not syspath.exists():
        raise ValueError(f"Could not find file/folder {syspath}.")
    elif syspath.is_dir():
        filepaths = [p for p in syspath.glob("**/*") if p.is_file()]
        if len(filepaths) == 0:
            raise ValueError(f"Could not find any files in {syspath}.")
    else:
        filepaths = [syspath]

    if len(filepaths) > 500:
        # TODO: better catching whether the user is resolving the whole dang base path
        raise ValueError("Found too many files matching {syspath}. ")

    # upload filepaths to a temp table, and join to local_files_flat
    with pg_connect() as conn:
        cur = conn.cursor()
        cur.execute("create temp table tmp_filepaths (filepath text)")
        cur.executemany(
            "insert into tmp_filepaths values (%s)", [(str(p),) for p in filepaths]
        )

        sql = f"""
        select local_files_flat.filepath
        from {schema}.local_files_flat
        inner join tmp_filepaths
            on tmp_filepaths.filepath like '%' || local_files_flat.filepath
        """
        cur.execute(sql)
        local_paths = [Path(r) for (r,) in cur]

    if not local_paths:
        raise ValueError(f"Could not find any matches to {syspath} in database.")

    # remove the local path part from the filepath.
    # this is easy if its a file, as we only need to remove the local path at the end.
    # if a directory, we need to remove the ending match. like: a/b/c, a/b/c/d.e -> a/b
    if syspath.is_dir():
        base_dir = None
        assert (
            len(list(local_paths[0].parents)) > 1
        )  # user would need to be resolving the base path for this to happen

        for parent in local_paths[0].parents:
            if str(syspath).endswith(str(parent)):
                base_dir = Path(str(syspath)[: -len(str(parent))])
                break

        if base_dir is None:
            raise ValueError(f"Could not find a base directory for {syspath}.")
    else:
        base_dir = Path(str(syspath)[: -len(str(local_paths[0]))])

    return base_dir, local_paths


def render_playlist(
    files: List[Path], out: str, outfile: Path = None, **plist_kw
) -> None:
    """Render an xspf playlist to stdout or a file/program.

    Supported output formats are:

        - stdout: print xspf xml string
        - file: write xspf xml string to file. must supply out_file
        - strawberry: load directly into the strawberry player
    """
    playlist = xspf.Playlist(
        trackList=[xspf.Track(location=str(p)) for p in files], **plist_kw
    )

    if out == "stdout":
        click.echo(playlist.xml_string())

    elif out == "file":
        if outfile is None:
            raise ValueError("Must supply out_file when outputting to file.")
        outfile.write_text(playlist.xml_string())

    elif out == "strawberry":
        with tempfile.NamedTemporaryFile() as f:
            fp = Path(f.name)
            fp.write_text(playlist.xml_string())
            subprocess.run(["strawberry", "--load", f.name])
            time.sleep(0.5)
    else:
        raise ValueError(f"Unknown output format {out}")
