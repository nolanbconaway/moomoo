"""Ingest musicbrainz data dumps from the live data api.

Store in the db a record of the dump, as well as records of which mbids were updated in the dump.
Docs at: https://metabrainz.org/api/

"""

import dataclasses
import datetime
import json
import os
import tarfile
from io import BytesIO
from typing import Optional

import click
import requests
from sqlalchemy.orm import Session
from tqdm import tqdm

from .db import MusicBrainzDataDump, MusicBrainzDataDumpRecord, get_session
from .utils_ import ENTITIES, request_with_retry

API_BASE = "https://metabrainz.org/api/musicbrainz"


def get_token_from_env() -> str:
    token = os.getenv("METABRAINZ_LIVE_DATA_TOKEN")
    if not token:
        raise RuntimeError(
            "METABRAINZ_LIVE_DATA_TOKEN env var must be set to access live data dumps."
        )
    return token


def parse_json_objects(s):
    """Chatgpt'd this function to parse multiple json objects from a string."""
    decoder = json.JSONDecoder()
    idx = 0
    n = len(s)

    while idx < n:
        # Skip whitespace
        while idx < n and s[idx].isspace():
            idx += 1
        if idx >= n:
            break

        obj, end = decoder.raw_decode(s, idx)
        yield obj
        idx = end  # continue after the parsed object


def get_latest_packet_number_from_api() -> int:
    resp = request_with_retry(
        method="GET", url=f"{API_BASE}/replication-info", params={"token": get_token_from_env()}
    )

    last_packet = resp.json()["last_packet"]

    # last packet is a filename like "replication-182019.tar.bz2". get the number from the filename
    # by takinng the last part before the extension
    packet_number = int(last_packet.split("-")[-1].split(".")[0])
    return packet_number


def get_latest_packet_number_from_db(session: Session, entity: str) -> Optional[int]:
    """Get the latest packet number stored in the db, or None if no dumps are stored."""
    latest_dump: Optional[MusicBrainzDataDump] = (
        session.query(MusicBrainzDataDump)
        .filter(MusicBrainzDataDump.entity == entity)
        .order_by(MusicBrainzDataDump.packet_number.desc())
        .first()
    )
    if latest_dump is None:
        return None
    return latest_dump.packet_number


def drop_dumps(session: Session, max_age_days: int) -> None:
    """Drop dumps older than the given number of days."""
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=max_age_days)
    old_dumps = (
        session.query(MusicBrainzDataDump).filter(MusicBrainzDataDump.dump_timestamp < cutoff).all()
    )
    for dump in old_dumps:
        click.echo(f"Dropping old dump {dump.slug} from db.")
        session.query(MusicBrainzDataDumpRecord).filter(
            MusicBrainzDataDumpRecord.slug == dump.slug
        ).delete(synchronize_session=False)
        session.delete(dump)
        session.commit()


@dataclasses.dataclass
class DataDump:
    packet_number: int
    entity: str
    timestamp: datetime.datetime
    records: list[dict]

    @classmethod
    def download(cls, entity: str, packet_number: int) -> Optional["DataDump"]:
        """Request a MusicBrainz data dump for a given entity and packet number."""
        url = f"{API_BASE}/json-dumps/json-dump-{packet_number}/{entity}.tar.xz"
        try:
            resp = request_with_retry(method="GET", url=url, params={"token": get_token_from_env()})
        except Exception as e:
            # if 404 due to no dump existing, return None
            if (
                isinstance(e, requests.exceptions.HTTPError)
                and e.response.status_code == 404
                and "Can't find specified JSON dump" in e.response.text
            ):
                return None
            raise

        # response is a tar.xz file.
        #
        # get the TIMESTAMP file within it and parse the timestamp
        # get the contents of the mbdump/<entity> file within it
        with tarfile.open(fileobj=BytesIO(resp.content), mode="r:xz") as tar:
            entity_member = tar.getmember(f"mbdump/{entity}")
            entity_fileobj = tar.extractfile(entity_member)
            if entity_fileobj is None:
                raise RuntimeError(f"Could not extract {entity} from datadump tarball.")

            ts_member = tar.getmember("TIMESTAMP")
            ts_fileobj = tar.extractfile(ts_member)
            if ts_fileobj is None:
                raise RuntimeError("Could not extract TIMESTAMP from datadump tarball.")

            timestamp = ts_fileobj.read()
            records = entity_fileobj.read()

        # read json lines. some lines have \n char so use a special stream reader.

        records = [record for record in parse_json_objects(records.decode()) if "id" in record]
        # timestamp is formatted like '2025-12-04 23:31:59.823155+00', which is not isoformat.
        # drop the tz and just assume utc

        timestamp = datetime.datetime.strptime(
            timestamp.decode("utf-8").strip().split("+")[0], "%Y-%m-%d %H:%M:%S.%f"
        )
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

        return cls(packet_number=packet_number, entity=entity, timestamp=timestamp, records=records)

    @property
    def slug(self) -> str:
        return f"{self.packet_number}-{self.entity}"

    def to_db(self, session: Session) -> MusicBrainzDataDump:
        """Store the datadump in the db."""

        # check if the dump already exists. add a new one of not
        db_dump = session.get(MusicBrainzDataDump, self.slug)
        if db_dump is None:
            db_dump = MusicBrainzDataDump(
                slug=self.slug,
                packet_number=self.packet_number,
                entity=self.entity,
                dump_timestamp=self.timestamp,
            )
            session.add(db_dump)
            session.commit()

        if len(self.records) == 0:
            click.echo(
                f"WARN: No records in dump for {db_dump.slug}, skipping.", err=True, color="yellow"
            )

        db_dump.replace_records(
            session=session,
            records=[
                dict(
                    mbid=record["id"],
                    json_data=dict(
                        containers=self.get_containers(record=record),
                    ),
                )
                for record in self.records
            ],
        )

        return db_dump

    def get_containers(self, record: dict) -> list[dict]:
        """Get the containers for a given record.

        E.g., if a release-group, get the arists to which it belongs and return them as a list of
        dicts like

            [
                {"mbid": "...", "entity": "artist"}
                ...
            ]
        """
        try:
            if self.entity == "release-group":
                return [
                    dict(mbid=i["artist"]["id"], entity="artist") for i in record["artist-credit"]
                ]

            if self.entity == "release":
                release_group = record["release-group"]["id"]
                artists = [i["artist"]["id"] for i in record["artist-credit"]]
                containers = []
                containers.append(dict(mbid=release_group, entity="release-group"))
                for artist in artists:
                    containers.append(dict(mbid=artist, entity="artist"))

                return containers

            # not doing anything for releases or artists yet...
        except Exception:
            print(record)
            raise

        return []


@click.command()
@click.option(
    "--drop-age-days",
    type=int,
    default=None,
    help="If set, drop dumps older than this many days from the db",
)
@click.option("--packet", type=int, default=None, help="Specific packet number to download.")
@click.option(
    "--entity",
    "entities",
    type=click.Choice(ENTITIES),
    multiple=True,
    default=ENTITIES,
    help="Entities for which to download dumps.",
)
def main(drop_age_days: Optional[int], packet: Optional[int], entities: list[str]) -> None:
    click.echo("Starting MusicBrainz data dump collection...")
    api_latest = get_latest_packet_number_from_api()
    click.echo(f"Latest packet number from API: {api_latest}")

    with get_session() as session:
        entity_latest = {
            entity: get_latest_packet_number_from_db(session, entity) for entity in entities
        }

    # Drop old dumps if requested
    if drop_age_days is not None:
        drop_dumps(session=session, max_age_days=drop_age_days)

    for entity in entities:
        db_latest = entity_latest[entity]
        if db_latest is None:
            # dumps are hourly, so start 7 days back if no dumps exist
            db_latest = api_latest - (7 * 24 + 1)  # +1 because we want to start exactly 7 days ago

        packets = [packet] if packet is not None else list(range(db_latest + 1, api_latest + 1))
        if not packets:
            click.echo(f"No new dumps to download for entity {entity}.")
            continue

        click.echo(f"Downloading {len(packets)} dumps for entity {entity}.")
        for packet_number in tqdm(packets, disable=None):
            click.echo(f"Downloading dump for {packet_number}-{entity}...")
            try:
                dump = DataDump.download(entity=entity, packet_number=packet_number)
            except Exception:
                click.echo(
                    f"ERROR: Failed to download dump for {packet_number}-{entity}",
                    color="red",
                    err=True,
                )
                raise

            if dump is None:
                click.echo(
                    f"WARN: No dump found for {packet_number}-{entity}, skipping.",
                    color="yellow",
                    err=True,
                )
                continue

            with get_session() as session:
                dump.to_db(session=session)

    click.echo("MusicBrainz data dump collection complete.")
