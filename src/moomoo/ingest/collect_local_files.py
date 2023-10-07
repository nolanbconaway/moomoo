"""Scan local files and add the metadata to the database."""
import multiprocessing
import sys
from pathlib import Path

import click
import mutagen
from tqdm.auto import tqdm

from .. import utils_
from ..db import LocalFile, get_session

EXTENSIONS: set[str] = set([".mp3", ".flac", ".ogg", ".opus", ".wav"])

# I manually looked at all the tags in my library and grouped semantically similar tags
# together here. Likely there are more tags that could be added.
ATTRIBUTES: dict[str, list[str]] = dict(
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


def list_audio_files(*dirs: Path) -> list[Path]:
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
        json_data=data,
    )


@click.command()
@click.argument(
    "src_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option("--procs", help="Number of processes to use", default=1, type=int)
def main(
    src_dir: list[Path],
    procs: int,
):
    """Ingest data from local files."""
    if not LocalFile.exists():
        click.echo(
            f"Table {LocalFile.table_name()} does not exist. "
            + f"Use `moomoo db create {LocalFile.table_name()}` to create it."
        )
        sys.exit(1)

    # get the list of files
    files = list_audio_files(src_dir)
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
    with get_session() as session:
        click.echo(f"Deleting all rows in {LocalFile.full_name()}")
        deleted = session.query(LocalFile).delete()
        click.echo(f"Deleted {deleted} rows")

        click.echo(f"Inserting {len(files)} files into {LocalFile.full_name()}")
        rows = [
            dict(
                filepath=str(path.relative_to(src_dir)),
                insert_ts_utc=utils_.utcnow(),
                **data,
            )
            for path, data in zip(files, parsed)
        ]
        LocalFile.bulk_insert(rows, session=session)

    click.echo("Done.")


if __name__ == "__main__":
    main()
