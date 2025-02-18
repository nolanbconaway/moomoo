"""Ingest a summary of data from the ListenBrainz data dumps.

These data are too large for me to have an appetite to store locally (for now), so instead i am just
going to summarize them at the user-recording level, and store that in the db.

Docs: https://listenbrainz.org/data/
"""

import dataclasses
import datetime
import json
import multiprocessing
import sys
import tarfile
import time
from collections import Counter
from functools import cache
from io import BytesIO
from typing import Optional
from urllib.parse import urlparse
from uuid import UUID

import click
import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from .db import ListenBrainzDataDump, get_session

BACK_DAYS = 30
BASE_URL = "http://ftp.musicbrainz.org/pub/musicbrainz/listenbrainz/incremental"
TIMEOUT = 5


def sleep() -> None:
    """Sleep for a short time."""
    time.sleep(TIMEOUT)


@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(TIMEOUT),
)
def request_with_retry(url, *args, **kwargs) -> requests.Response:
    """Retry a request."""
    r = requests.get(url, *args, **kwargs)
    r.raise_for_status()
    return r


@cache
def request_data_dump(url: str) -> BytesIO:
    """Request the data dump."""
    click.echo(f"Requesting {url}")
    resp = request_with_retry(url, stream=True)
    fh = BytesIO()
    for chunk in resp.iter_content(chunk_size=1024):
        fh.write(chunk)
    fh.seek(0)
    return fh


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
    url: str  # # {BASE_URL}/a/b/listenbrainz-listens-dump-1978-20250125-000003-incremental.tar.xz

    @classmethod
    def fetch_list(cls, dates: list[datetime.date] | None = None) -> list["DataDump"]:
        """Fetch the list of data dumps.

        Toplevel keys are listed as <a> tags in a <pre>, and will have hrefs like
        listenbrainz-dump-1972-20250117-000003-incremental. We then need to parse the date out of
        the href.

        The data dump is stored in a file under that url as a .tar.xz file; and that is the url that
        we need.
        """
        click.echo(f"Listing {BASE_URL}")
        resp = request_with_retry(BASE_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        urls = [
            i["href"]
            for i in soup.find("pre").find_all("a")
            if i["href"].startswith("listenbrainz-dump-")
        ]
        url_dates = [datetime.datetime.strptime(i.split("-")[-3], "%Y%m%d").date() for i in urls]
        # filter by date
        if dates:
            urls = [i for i, j in zip(urls, url_dates) if j in dates]
            url_dates = [i for i in url_dates if i in dates]

        # drop tail / from the url
        urls = [i.rstrip("/") for i in urls]

        click.echo(f"Listing dumps on dates: {url_dates}")

        # now we need to find the correct listens archive within each url. there will be a spark url
        # and a listens url, and we want the listens url.
        tarball_urls = []
        for url in urls:
            sleep()
            click.echo(f"Listing objects at {BASE_URL}/{url}")
            resp = request_with_retry(f"{BASE_URL}/{url}")
            resp.raise_for_status()
            soup = BeautifulSoup(resp.content, "html.parser")
            tarball_url = next(
                (
                    i["href"]
                    for i in soup.find("pre").find_all("a")
                    if i["href"].endswith(".tar.xz")
                    and "spark" not in i["href"]
                    and "listens" in i["href"]
                ),
                None,
            )
            if url:
                tarball_urls.append(f"{BASE_URL}/{url}/{tarball_url}")

        return [cls(url=i) for i in tarball_urls]

    @property
    def filename(self) -> str:
        """The filename of the dump tarball."""
        return urlparse(self.url).path.split("/")[-1]

    @property
    def date(self) -> datetime.date:
        """The date of the dump."""
        # listenbrainz-listens-dump-1978-20250125-000003-incremental.tar.xz
        return datetime.datetime.strptime(self.filename.split("-")[-3], "%Y%m%d").date()

    @property
    def data(self) -> BytesIO:
        """Get the data from the dump."""
        return request_data_dump(self.url)

    def get_start_timestamp(self) -> datetime.datetime:
        """Get the START_TIMESTAMP file value from the dump.

        Should be a value like 2025-01-29 13:31:09+00:00
        """
        self.data.seek(0)
        with tarfile.open(fileobj=self.data, mode="r:xz") as tar:
            ts = next(
                tar.extractfile(i).read().decode("utf-8").strip()
                for i in tar.getmembers()
                if i.name.endswith("START_TIMESTAMP")
            )

        return datetime.datetime.fromisoformat(ts)

    def get_end_timestamp(self) -> datetime.datetime:
        """Get the END_TIMESTAMP file value from the dump.

        Should be a value like 2025-01-29 13:31:09+00:00
        """
        self.data.seek(0)
        with tarfile.open(fileobj=self.data, mode="r:xz") as tar:
            end_timestamp = next(
                tar.extractfile(i).read().decode("utf-8").strip()
                for i in tar.getmembers()
                if i.name.endswith("END_TIMESTAMP")
            )
        return datetime.datetime.fromisoformat(end_timestamp)

    def get_listens(self, procs: int = 100) -> list[Listen]:
        """Get the listens file from the dump.

        This will be a file like <number>.listens, which will be plaintext json lines.
        """
        self.data.seek(0)
        with tarfile.open(fileobj=self.data, mode="r:xz") as tar:
            member = next((m for m in tar.getmembers() if m.name.endswith(".listens")), None)
            if not member:
                click.echo(f"No listens file found in {self.filename}")
                return []
            click.echo(f"Extracting {member.name} from {self.filename} with {procs} procs")
            with tar.extractfile(member) as f, multiprocessing.Pool(procs) as pool:
                res = []
                for i in tqdm(pool.imap(Listen.from_line, f, chunksize=100), disable=None):
                    res += i
                return res


def get_known_data_dumps(session: Session) -> list[DataDump]:
    """Get the known data dumps from the database."""
    return [DataDump(i.url) for i in session.query(ListenBrainzDataDump).all()]


@click.command()
@click.option("--procs", type=int, default=1)
@click.option("--date", "dates", type=click.DateTime(formats=["%Y-%m-%d"]), multiple=True)
def main(procs: int, dates: list[datetime.datetime]) -> None:
    if dates:
        dates = [date.date() for date in dates]  # click.DateTime returns a datetime object
    else:
        # if no date, get any date for which there are no dumps in the last BACK_DAYS days
        with get_session() as session:
            known_dates = set(i.date for i in get_known_data_dumps(session))

            # get any date from the last BACK_DAYS days for which there is no dump
            dates = [
                datetime.date.today() - datetime.timedelta(days=i)
                for i in range(BACK_DAYS)
                if datetime.date.today() - datetime.timedelta(days=i) not in known_dates
            ]

    # get the data dumps for the given dates
    dumps = DataDump.fetch_list(dates=dates)

    click.echo(f"Found {len(dumps)} data dumps to process.")
    if not dumps:
        click.echo("No data dumps to process.")
        sys.exit(0)

    for dump in dumps:
        click.echo(f"Processing {dump.filename}...")
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
                .filter(ListenBrainzDataDump.url == dump.url)
                .one_or_none()
            )

            if db_dump is None:
                db_dump = ListenBrainzDataDump(
                    url=dump.url,
                    date=dump.date,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                )
                session.add(db_dump)
                session.commit()

            click.echo(f"Replacing all rows for {dump.filename}")
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

            click.echo(f"Processed {dump.filename}")

    click.echo("Done.")
