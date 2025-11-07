"""Query the ListenBrainz listens api to map local files to msids.

Requires that the LocalFile table has been populated, as we need to use the file
metadata for artist and track names.

Because track metadata can change, we map msids to a hash of the artist and track name.
We do this for the artist name and album artist name, as it is not clear which one is
scrobbled.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/metadata.html#get--1-metadata-lookup-
"""

import datetime
import random
import sys
from typing import Optional

import click
from liblistenbrainz import ListenBrainz
from liblistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from . import utils_
from .db import LocalFile, MessyBrainzNameMap, execute_sql_fetchall, get_session

# base sql to extract artist names and hashes from the local files table
RECORDINGS_BASE = f"""
select distinct recording_md5, recording_name, release_name, artist_name
from {LocalFile.table_name()}
where recording_md5 is not null
  -- max is 250 but i am not sure if & signs are counted, etc.
  and length(concat(artist_name, recording_name, release_name)) < 245
"""


def get_new_recordings() -> list[dict]:
    """Get files that have not been mapped yet."""
    sql = f"""
        with base as ( {RECORDINGS_BASE} )
        select base.*
        from base
        left join {MessyBrainzNameMap.table_name()} as src using (recording_md5)
        where src.recording_md5 is null
    """
    return execute_sql_fetchall(sql)


def get_old_recordings(before: datetime.datetime) -> list[dict]:
    """Get files that were mapped before a date.

    This is to catch cases where listenbrainz may have changed the metadata for a
    recording.
    """
    sql = f"""
        with base as ( {RECORDINGS_BASE} )
        select base.*
        from base
        inner join {MessyBrainzNameMap.table_name()} as src using (recording_md5)
        where src.ts_utc < :before
    """
    return execute_sql_fetchall(sql, params=dict(before=before))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ListenBrainzAPIException),
    reraise=True,
)
def lookup_msid(recording_name: str, release_name: str, artist_name: str) -> dict:
    """Lookup data for a recording."""
    client = ListenBrainz()
    endpoint = "/1/metadata/lookup/"

    return client._get(
        endpoint,
        params={
            "artist_name": artist_name,
            "recording_name": recording_name,
            "release_name": release_name,
            "metadata": True,
            "inc": "release",  # to get release group
        },
    )


@click.command(help=__doc__)
@click.option(
    "--new",
    "new_",
    is_flag=True,
    help="Option to detect recordings that have not been mapped yet.",
)
@click.option(
    "--before",
    "before",
    type=utils_.utcfromisodate,
    help="Option to re-map recordings last ingested since this date.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Limit the number of recordings to map.",
    default=None,
)
def main(new_: bool, before: Optional[datetime.datetime], limit: Optional[int]):
    """Run the main CLI."""
    # get list of mbids to annotate
    to_ingest: list[str] = []
    if new_:
        click.echo("Getting recordings with no mapping...")
        to_ingest += get_new_recordings()
        click.echo(f"Found {len(to_ingest)} new recording(s).")
    if before:
        click.echo(f"Getting recordings to re-ingest (before {before})...")
        to_ingest += get_old_recordings(before)
        click.echo(f"Found {len(to_ingest)} old recording(s).")

    # exit if there is nothing to do
    click.echo(f"Found {len(to_ingest)} total recording(s) to ingest.")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # limit if needed
    if limit and len(to_ingest) > limit:
        click.echo(f"Limiting to {limit} recording(s) randomly.")
        to_ingest = random.sample(to_ingest, k=limit)

    # annotate and insert
    click.echo("ingesting...")
    with get_session() as session:
        for recording in tqdm(to_ingest, disable=None, total=len(to_ingest)):
            res = lookup_msid(
                recording_name=recording["recording_name"],
                release_name=recording["release_name"],
                artist_name=recording["artist_name"],
            )
            MessyBrainzNameMap(
                **recording,
                success="recording_name" in res,
                payload_json=res,
                ts_utc=utils_.utcnow(),
            ).upsert(session=session)

    click.echo("Done.")


if __name__ == "__main__":
    main()
