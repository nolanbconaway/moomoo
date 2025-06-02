"""Ingest a summary of data from the ListenBrainz data dumps.

These data are too large for me to have an appetite to store locally (for now), so instead i am just
going to summarize them at the user-recording level, and store that in the db.

Docs: https://listenbrainz.org/data/
"""

import contextlib
import dataclasses
import datetime
import ftplib
import json
import multiprocessing
import sys
import tarfile
from collections import Counter
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional
from uuid import UUID

import click
import zstandard
from tqdm import tqdm

from .db import ListenBrainzDataDump, get_session

FTP_HOST = "ftp.musicbrainz.org"
FTP_DIR = "/pub/musicbrainz/listenbrainz/incremental"
FTP_PORT = 21


@contextmanager
def ftp_session() -> Generator[ftplib.FTP, None, None]:
    """Context manager for FTP session.

    Yields a ftp connection object set to the relevant directory.
    """
    try:
        click.echo(f"Connecting to FTP: {FTP_HOST}")
        ftp = ftplib.FTP()
        ftp.connect(host=FTP_HOST, port=FTP_PORT)
        ftp.login()
        ftp.cwd(FTP_DIR)
        yield ftp
    finally:
        click.echo(f"Closing FTP connection to {FTP_HOST}")
        try:
            ftp.quit()
        except Exception as e:
            click.echo(f"Error closing FTP connection: {e}. Force closing the connection.")
            ftp.close()


def try_uuid(s: str) -> Optional[UUID]:
    """Try to parse a string as a UUID."""
    try:
        return UUID(s)
    except ValueError:
        return None


@dataclasses.dataclass(order=True, slots=True, frozen=True)
class Listen:
    user_id: int
    artist_mbid: UUID

    @classmethod
    def from_line(cls, line: str) -> list["Listen"]:
        """Parse a line from the ListenBrainz data dump."""
        # some example lines. we want the user id (which should always be present), and the artist
        # mbids (which can be missing, or can be called lastfm_artist_mbid, artist_mbids, or
        # artist_mbid).
        #
        # {"user_id": 12345, "track_metadata": {"additional_info": { "artist_mbids": [""]}}}
        # {"user_id": 12345, "track_metadata": {"additional_info": { "artist_mbid": ""}}}
        #
        # we want the mbids first, then the mbid, then the lastfm_artist_mbid
        data = json.loads(line)
        user_id = int(data["user_id"])

        # try to get the artist mbid from the track_metadata
        artist_mbids = data["track_metadata"]["additional_info"].get("artist_mbids")
        artist_mbid = data["track_metadata"]["additional_info"].get("artist_mbid")

        if artist_mbids:
            artist_mbids = [try_uuid(i) for i in artist_mbids]
        elif artist_mbid:
            artist_mbids = [try_uuid(artist_mbid)]
        else:
            artist_mbids = []

        # filter out None values
        artist_mbids = tuple([i for i in artist_mbids if i is not None])

        return [cls(user_id=user_id, artist_mbid=i) for i in artist_mbids]


@dataclasses.dataclass
class DataDump:
    ftp_path: Path
    modify_ts: datetime.datetime

    @classmethod
    def fetch_list(cls) -> list["DataDump"]:
        """Fetch the list of data dumps currently available on the FTP server."""
        click.echo(f"Listing data dumps from {FTP_HOST}{FTP_DIR}")
        results = []
        with ftp_session() as ftp:
            for base_dir in tqdm(list(ftp.nlst(FTP_DIR)), disable=None):
                # get the first file that is like listenbrainz-listens-dump-...-incremental.tar.xz
                filepath = next(
                    (
                        Path(i)
                        for i in sorted(list(ftp.nlst(base_dir)))
                        if i.endswith(".tar.zst") and "spark" not in i and "listens-" in i
                    ),
                    None,
                )
                if filepath:
                    ts = datetime.datetime.strptime(
                        ftp.voidcmd(f"MDTM {filepath}")[4:].strip(),
                        "%Y%m%d%H%M%S",
                    )
                    ts = ts.replace(tzinfo=datetime.timezone.utc)  # Ensure UTC timezone
                    results.append(cls(ftp_path=Path(filepath), modify_ts=ts))

        return results

    @property
    def slug(self) -> str:
        """Get the slug for the data dump.

        This is equivalent to the base directory name for the dump, which is the part of the path.
        Like: {FTP_DIR}/{slug}/{filename}.tar.xz
        It will be a value like: listenbrainz-dump-2054-20250301-000003-incremental
        """
        return self.ftp_path.relative_to(Path(FTP_DIR)).parts[0]

    @property
    def date(self) -> datetime.date:
        """The date of the dump."""
        return datetime.datetime.strptime(self.slug.split("-")[3], "%Y%m%d").date()

    @property
    def data(self) -> BytesIO:
        """Fetch the data from the FTP server.

        Data are cached in the _data attribute after the first fetch to avoid multiple calls to the
        FTP server.
        """
        if hasattr(self, "_data"):
            # if we already fetched the data, return it
            self._data.seek(0)  # Reset the buffer to the beginning
            return self._data

        # otherwise, fetch the data from the FTP server
        click.echo(f"Fetching data for {self.ftp_path} from FTP server...")
        data = BytesIO()
        with ftp_session() as ftp:
            ftp.retrbinary(f"RETR {self.ftp_path}", data.write)
        data.seek(0)  # Reset the buffer to the beginning
        click.echo(f"Successfully fetched data for {self.ftp_path}")

        # Store the data in the instance for future use
        self._data = data
        return self._data

    @property
    @contextlib.contextmanager
    def tarfile(self) -> Generator[tarfile.TarFile, None, None]:
        """Get a context manager for the tarfile."""
        decompressor = zstandard.ZstdDecompressor()
        with decompressor.stream_reader(self.data, closefd=False) as reader:
            data = BytesIO(reader.read())
            data.seek(0)
            with tarfile.open(fileobj=data, mode="r:") as tar:
                yield tar

    def get_start_timestamp(self) -> datetime.datetime:
        """Get the START_TIMESTAMP file value from the dump.

        Should be a value like 2025-01-29 13:31:09+00:00
        """
        with self.tarfile as tar:
            ts = next(
                f.read().decode("utf-8").strip()
                for i in tar.getmembers()
                if i.name.endswith("START_TIMESTAMP")
                for f in [tar.extractfile(i)]
                if f is not None
            )

        return datetime.datetime.fromisoformat(ts)

    def get_end_timestamp(self) -> datetime.datetime:
        """Get the END_TIMESTAMP file value from the dump.

        Should be a value like 2025-01-29 13:31:09+00:00
        """
        with self.tarfile as tar:
            end_timestamp = next(
                f.read().decode("utf-8").strip()
                for i in tar.getmembers()
                if i.name.endswith("END_TIMESTAMP")
                for f in [tar.extractfile(i)]
                if f is not None
            )
        return datetime.datetime.fromisoformat(end_timestamp)

    def get_listens(self, procs: int = 100) -> list[Listen]:
        """Get the listens file from the dump.

        This will be a file like <number>.listens, which will be plaintext json lines.
        """
        with self.tarfile as tar:
            member = next((m for m in tar.getmembers() if m.name.endswith(".listens")), None)
            if not member:
                click.echo(f"No listens file found in {self.ftp_path}")
                return []
            click.echo(f"Extracting {member.name} from {self.ftp_path} with {procs} procs")
            with tar.extractfile(member) as f, multiprocessing.Pool(procs) as pool:
                res = []
                for i in tqdm(pool.imap(Listen.from_line, f, chunksize=100), disable=None):
                    res += i
                return res


def get_known_data_dumps() -> list[DataDump]:
    """Get the known data dumps from the database."""
    with get_session() as session:
        return [
            DataDump(ftp_path=Path(i.ftp_path), modify_ts=i.ftp_modify_ts)
            for i in session.query(ListenBrainzDataDump).all()
        ]


@click.command()
@click.option("--procs", type=int, default=1)
def main(procs: int) -> None:
    # get the data dumps for the given dates
    ftp_dumps = DataDump.fetch_list()
    db_dumps = {dump.slug: dump for dump in get_known_data_dumps()}

    click.echo(f"Found {len(ftp_dumps)} data dumps to process.")
    if not ftp_dumps:
        click.echo("No data dumps to process.")
        sys.exit(0)

    # process any data dumps which are:
    #  - any new dumps (unknown slug)
    #  - any dumps with modify ts that are newer than in the db
    ftp_dumps = [
        ftp_dump
        for ftp_dump in ftp_dumps
        if ftp_dump.slug not in db_dumps or db_dumps[ftp_dump.slug].modify_ts < ftp_dump.modify_ts
    ]

    for dump in ftp_dumps:
        click.echo(f"Processing {dump.ftp_path}...")
        listens = dump.get_listens(procs=procs)
        start_timestamp = dump.get_start_timestamp()
        end_timestamp = dump.get_end_timestamp()
        click.echo(f"Start timestamp: {start_timestamp}")
        click.echo(f"End timestamp: {end_timestamp}")
        click.echo(f"Got {len(listens)} listens.")

        if not listens:
            click.echo("No listens to process.")
            continue

        # get or add the dump
        with get_session() as session:
            db_dump = (
                session.query(ListenBrainzDataDump)
                .filter(ListenBrainzDataDump.slug == dump.slug)
                .one_or_none()
            )

            if db_dump is None:
                db_dump = ListenBrainzDataDump(
                    slug=dump.slug,
                    ftp_path=str(dump.ftp_path),
                    ftp_modify_ts=dump.modify_ts,
                    date=dump.date,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                )
                session.add(db_dump)
                session.commit()

            click.echo(f"Replacing all rows for {dump.ftp_path}")
            db_dump.replace_records(
                session=session,
                records=[
                    dict(
                        user_id=listen.user_id,
                        artist_mbid=listen.artist_mbid,
                        listen_count=count,
                    )
                    for listen, count in Counter(listens).items()
                ],
            )

            click.echo(f"Processed {dump.ftp_path}")

    click.echo("Done.")


if __name__ == "__main__":
    main()
