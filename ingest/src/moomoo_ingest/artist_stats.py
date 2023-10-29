"""Query the ListenBrainz listens api for alltime stats per artist.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/statistics.html#get--1-stats-artist-(artist_mbid)-listeners

"""
import datetime
import json
import os
import random
import sys
import uuid
from typing import Optional

import click
from pylistenbrainz import ListenBrainz
from pylistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from . import utils_
from .db import ListenBrainzArtistStats, execute_sql_fetchall, get_session


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ListenBrainzAPIException),
    reraise=True,
)
def _get_artist_stats(mbid: uuid.UUID) -> dict:
    """Get global listen stats for an entity.

    Internal method wrapping retries, etc.
    """
    client = ListenBrainz()
    endpoint = f"/1/stats/artist/{mbid}/listeners"
    try:
        return client._get(endpoint, params={"range": "all_time"})["payload"]
    except ListenBrainzAPIException as e:
        if e.status_code == 204:  # no data in range
            return dict()
        raise e


def get_artist_stats(mbid: uuid.UUID) -> dict:
    """Get global listen stats for an entity."""
    try:
        data = _get_artist_stats(mbid)
        error = None if data else "HTTP 204: No data in range."
    except ListenBrainzAPIException as e:
        data = None
        error = str(e)

    return dict(success=error is None, error=error, data=data)


def get_new_mbids() -> list[uuid.UUID]:
    """Get mbids that have nostats from the mbids table."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid as mbid
        from {dbt_schema}.mbids
        left join {ListenBrainzArtistStats.table_name()} as src on mbids.mbid = src.mbid
        where src.mbid is null
          and mbids.entity = 'artist'
    """
    return [i["mbid"] for i in execute_sql_fetchall(sql)]


def get_old_mbids(before: datetime.datetime) -> list[uuid.UUID]:
    """Get mbids with stats between from_dt and to_dt."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid
        from {dbt_schema}.mbids
        inner join {ListenBrainzArtistStats.table_name()} as src
            on mbids.mbid::varchar = src.mbid::varchar
        where src.ts_utc < :before and mbids.entity = 'artist'
        order by src.ts_utc
    """
    params = dict(before=before)
    return [i["mbid"] for i in execute_sql_fetchall(sql, params=params)]


@click.command(help=__doc__)
@click.option(
    "--new",
    "new_",
    is_flag=True,
    help="Option to detect new mbids that have not been annotated yet.",
)
@click.option(
    "--before",
    "before",
    type=utils_.utcfromisodate,
    help="Upper bound on last ingested artist stats to re-ingest.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Limit the number of mbids to annotate.",
    default=None,
)
def main(new_: bool, before: Optional[datetime.datetime], limit: Optional[int]):
    """Run the main CLI."""
    # get list of mbids to annotate
    to_ingest: list[str] = []
    if new_:
        click.echo("Getting mbids with no stats...")
        to_ingest += get_new_mbids()
    if before:
        click.echo(f"Getting mbids to re-ingest (before {before})...")
        to_ingest += get_old_mbids(before)

    # exit if there is nothing to do
    click.echo(f"Found {len(to_ingest)} mbid(s) to ingest.")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # limit if needed
    if limit and len(to_ingest) > limit:
        click.echo(f"Limiting to {limit} mbid(s) randomly.")
        to_ingest = random.sample(to_ingest, k=limit)

    # annotate and insert
    click.echo("ingesting...")
    with get_session() as session:
        for mbid, res in tqdm(
            zip(to_ingest, map(get_artist_stats, to_ingest)),
            disable=None,
            total=len(to_ingest),
        ):
            ListenBrainzArtistStats(
                mbid=mbid,
                payload_json=json.dumps(res),
                ts_utc=utils_.utcnow(),
            ).upsert(session=session)

    click.echo("Done.")


if __name__ == "__main__":
    main()
