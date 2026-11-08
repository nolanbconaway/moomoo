"""Query the ListenBrainz listens api for all-time stats per artist.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/statistics.html#get--1-stats-artist-(artist_mbid)-listeners

Requires that the musicbrainz sync has been run to populate the artist table, as we need to
list the relevant artist MBIDs.

This tool has logic to detect new mbids that have not been annotated yet, as well as
mbids that have been annotated but may need refreshing. The latter is useful for mbids
that were enriched long ago, but may have new data available in MusicBrainz.

In practice, it takes roughly ~0.5s to annotate a single mbid. Use this with the --limit
option to limit the total run time.
"""

import datetime
import random
import sys
import uuid

import click
from liblistenbrainz.errors import ListenBrainzAPIException
from moomoo_pg import ListenBrainzArtistStats, execute_sql_fetchall, get_session
from requests.exceptions import ConnectionError as RequestsConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from . import utils_

# global ListenBrainz client, rate limiting is handled internally
client = utils_.get_listenbrainz_client()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(
        (ListenBrainzAPIException, RequestsConnectionError, ConnectionError)
    ),
    reraise=True,
)
def _get_artist_stats(mbid: uuid.UUID) -> dict:
    """Get global listen stats for an entity.

    Internal method wrapping retries, etc.
    """
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
    # TODO: use the musicbrainz table object to reference the table name.
    sql = f"""
        select artist.gid as mbid
        from musicbrainz.artist
        left join {ListenBrainzArtistStats.table_name()} as src on artist.gid = src.mbid
        where src.mbid is null
    """
    return [i["mbid"] for i in execute_sql_fetchall(sql)]


def get_old_mbids(before: datetime.datetime) -> list[uuid.UUID]:
    """Get artists whose stats predate `before`."""
    sql = f"""
        select artist.gid as mbid
        from musicbrainz.artist as artist
        inner join {ListenBrainzArtistStats.table_name()} as src on artist.gid = src.mbid
        where src.ts_utc < :before
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
def main(new_: bool, before: datetime.datetime | None, limit: int | None):
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
            zip(to_ingest, map(get_artist_stats, to_ingest), strict=True),
            disable=None,
            total=len(to_ingest),
        ):
            ListenBrainzArtistStats(mbid=mbid, payload_json=res, ts_utc=utils_.utcnow()).upsert(
                session=session
            )
            session.commit()

    click.echo("Done.")


if __name__ == "__main__":
    main()
