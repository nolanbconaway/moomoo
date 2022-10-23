"""Scan local files and add the metadata to the database."""
import json
import multiprocessing
import sys
from pathlib import Path
from typing import List, Set

import click
import mutagen
from tqdm.auto import tqdm

from . import utils_

DDL = [
    """
    create table {schema}.{table} (
        filepath varchar not null primary key
        , json_data jsonb not null
        , file_created_at timestamp with time zone not null
        , file_modified_at timestamp with time zone not null
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
]

EXTENSIONS: Set[str] = set([".mp3", ".flac", ".ogg", ".opus", ".wav"])


def list_audio_files(*dirs: Path) -> List[Path]:
    """List all audio files in the directories."""
    return [
        p
        for d in dirs
        for p in d.rglob("**/*")
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]


def parse_audio_file(path: Path) -> dict:
    """Parse the audio file and return the metadata."""
    # NOTE: Platform dependent
    # https://docs.python.org/3/library/os.html#os.stat_result.st_ctime
    file_created_at = utils_.utcfromunixtime(path.stat().st_ctime)
    file_modified_at = utils_.utcfromunixtime(path.stat().st_mtime)
    try:
        audio = mutagen.File(path, easy=True)
        data = dict(
            artist=audio.get("artist", [""])[0] or None,
            album_artist=next(
                (
                    audio.get(i, [""])[0]
                    for i in ("album artist", "albumartist", "album_artist")
                    if audio.get(i, [""])[0]
                ),
                None,
            ),
            album=audio.get("album", [""])[0] or None,
            title=audio.get("title", [""])[0] or None,
            length=audio.info.length,
        )
    except mutagen.MutagenError:
        data = dict()

    return dict(
        file_created_at=file_created_at,
        file_modified_at=file_modified_at,
        json_data=json.dumps(data),
    )


def insert(conn, schema: str, table: str, filepath: Path, data: dict):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {schema}.{table} (
                filepath, json_data, file_created_at, file_modified_at
            )
            values (
                %(filepath)s, %(json_data)s, %(file_created_at)s, %(file_modified_at)s
            )
            on conflict (filepath) do update set
                json_data = excluded.json_data
                , file_created_at = excluded.file_created_at
                , file_modified_at = excluded.file_modified_at
            """,
            dict(filepath=str(filepath), **data),
        )


@click.command()
@click.argument("src_dir", type=click.Path(exists=True, file_okay=False), nargs=-1)
@click.option("--table", required=True)
@click.option("--schema", required=True)
@click.option("--procs", help="Number of processes to use", default=2, type=int)
@click.option(
    "--create", is_flag=True, help="Option to teardown and recreate the table"
)
def main(src_dir: List[Path], table: str, schema: str, procs: int, create: bool):
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    # get the list of files
    files = list_audio_files(*map(Path, src_dir))
    click.echo(f"Found {len(files)} audio files")

    if not files:
        click.echo("No audio files found. Exiting.")
        sys.exit(0)

    # parse the files
    click.echo("Parsing audio files")
    with multiprocessing.Pool(procs) as pool:
        parsed = list(
            tqdm(
                pool.imap(parse_audio_file, files, chunksize=5),
                total=len(files),
                disable=None,  # not sys.stdout.isatty(),
                mininterval=0.5,
            )
        )

    # insert the files
    with utils_.pg_connect() as conn:
        click.echo(f"""Inserting {len(files)} files into {schema}.{table}""")
        for path, data in tqdm(
            zip(files, parsed),
            disable=None,  # not sys.stdout.isatty(),
            mininterval=0.5,
            total=len(files),
        ):
            insert(conn=conn, schema=schema, table=table, filepath=path, data=data)

        conn.commit()


if __name__ == "__main__":
    main()
