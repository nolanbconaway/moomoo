"""Scan local files and add the metadata to the database."""
import json
import multiprocessing
import sys
from pathlib import Path
from typing import List, Set, Dict

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

# I manually looked at all the tags in my library and grouped semantically similar tags
# together here. Likely there are more tags that could be added.
ATTRIBUTES: Dict[str, List[str]] = dict(
    album=["album"],
    title=["title"],
    artist=["artist"],
    tracknumber=["tracknumber"],
    discnumber=["discnumber"],
    genre=["genre"],
    date=["date", "originalyear", "year", "origyear"],
    album_artist=["albumartist", "album artist"],
    musicbrainz_trackid=["musicbrainz_trackid"],
    musicbrainz_artistid=["musicbrainz_artistid"],
    musicbrainz_albumid=["musicbrainz_albumid"],
    musicbrainz_albumartistid=["musicbrainz_albumartistid"],
    musicbrainz_discid=["musicbrainz_discid"],
    musicbrainz_albumstatus=["musicbrainz_albumstatus"],
    musicbrainz_albumtype=["musicbrainz_albumtype"],
    musicbrainz_releasetrackid=["musicbrainz_releasetrackid"],
    musicbrainz_releasegroupid=["musicbrainz_releasegroupid"],
)


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
        data = {
            attr: next(
                (
                    audio.get(key, [""])[0]
                    for key in keys
                    # found a case where genre was set to []. so protect against that
                    if len(audio.get(key, [""])) > 0 and audio.get(key, [""])[0]
                ),
                None,
            )
            for attr, keys in ATTRIBUTES.items()
        }
        data["length"] = audio.info.length
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
@click.option("--procs", help="Number of processes to use", default=1, type=int)
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
    real_procs = max(min(procs, len(files)), 1)
    if real_procs == 1:
        # set disable=None for not sys.stdout.isatty(),
        click.echo("Parsing audio files serially")
        parsed = list(
            tqdm(map(parse_audio_file, files), total=len(files), disable=None)
        )
    else:
        click.echo(f"Parsing audio files in {real_procs} processes")
        with multiprocessing.Pool(real_procs) as pool:
            parsed = list(
                tqdm(
                    pool.imap(parse_audio_file, files, chunksize=5),
                    total=len(files),
                    disable=None,
                )
            )

    # insert the files
    with utils_.pg_connect() as conn:
        click.echo(f"""Inserting {len(files)} files into {schema}.{table}""")
        for path, data in tqdm(zip(files, parsed), disable=None, total=len(files)):
            insert(conn=conn, schema=schema, table=table, filepath=path, data=data)

        conn.commit()


if __name__ == "__main__":
    main()
