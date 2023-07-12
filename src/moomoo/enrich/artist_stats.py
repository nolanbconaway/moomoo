"""Query the ListenBrainz listens api for alltime stats per artist.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/statistics.html#get--1-stats-artist-(artist_mbid)-listeners

"""
import datetime
import json
import random
import sys
import uuid
from typing import List, Optional

import click
from psycopg import Connection
from pylistenbrainz import ListenBrainz
from pylistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from .. import utils_

DDL = [
    """
    create table {schema}.{table} (
        mbid uuid not null primary key,
        payload_json jsonb,
        ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
    "create index {schema}_{table}_ingest_ts_idx on {schema}.{table} (ts_utc)",
]


def insert(conn: Connection, schema: str, table: str, data: dict):
    sql = f"""
        insert into {schema}.{table} (mbid, payload_json) 
        values (%(mbid)s, %(payload_json)s)
        on conflict (mbid) do update set
            mbid = excluded.mbid
            , ts_utc = current_timestamp
            , payload_json = excluded.payload_json
    """
    with conn.cursor() as cur:
        cur.execute(sql, params=data)


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


def get_new_mbids(schema: str, table: str, dbt_schema: str) -> List[uuid.UUID]:
    """Get mbids that have nostats from the mbids table."""
    sql = f"""
        select mbids.mbid as mbid
        from {dbt_schema}.mbids
        left join {schema}.{table} as src on mbids.mbid = src.mbid
        where src.mbid is null
          and mbids.entity = 'artist'
    """
    return [i["mbid"] for i in utils_.execute_sql_fetchall(sql)]


def get_old_mbids(
    schema: str, table: str, dbt_schema: str, before: datetime.datetime
) -> List[uuid.UUID]:
    """Get mbids with stats between from_dt and to_dt."""
    sql = f"""
        select mbids.mbid
        from {dbt_schema}.mbids
        inner join {schema}.{table} as src
            on mbids.mbid::varchar = src.mbid::varchar
        where src.ts_utc < %(before)s and mbids.entity = 'artist'
        order by src.ts_utc
    """
    params = dict(before=before)
    return [i["mbid"] for i in utils_.execute_sql_fetchall(sql, params=params)]


@click.command(help=__doc__)
@click.option("--table", required=True, help="Table to store the annotated data.")
@click.option("--schema", required=True, help="Schema to store the annotated data.")
@click.option(
    "--dbt-schema",
    required=True,
    help="Schema where the dbt target is stored. Used to access the mbids table.",
)
@click.option(
    "--create", is_flag=True, help="Option to teardown and recreate the table"
)
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
def main(
    table: str,
    schema: str,
    dbt_schema: str,
    create: bool,
    new_: bool,
    before: Optional[datetime.datetime],
    limit: Optional[int],
):
    """Run the main CLI."""
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    get_artist_stats("509b8a09-e1cb-4ace-bbaf-296ee9701abd")
    # get list of mbids to annotate
    to_ingest: List[str] = []
    if new_:
        click.echo("Getting mbids with no stats...")
        to_ingest += get_new_mbids(schema=schema, table=table, dbt_schema=dbt_schema)
    if before:
        click.echo(f"Getting mbids to re-ingest (before {before})...")
        to_ingest += get_old_mbids(
            schema=schema, table=table, dbt_schema=dbt_schema, before=before
        )

    # exit if there is nothing to do
    click.echo(f"Found {len(to_ingest)} mbids to ingest.")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # limit if needed
    if limit and len(to_ingest) > limit:
        click.echo(f"Limiting to {limit} mbids randomly.")
        to_ingest = random.choices(to_ingest, k=limit)

    # annotate and insert
    click.echo("ingesting...")
    with utils_.pg_connect() as conn:
        for mbid, res in tqdm(
            zip(to_ingest, map(get_artist_stats, to_ingest)),
            disable=None,
            total=len(to_ingest),
        ):
            insert(
                conn=conn,
                schema=schema,
                table=table,
                data=dict(mbid=mbid, payload_json=json.dumps(res)),
            )
            conn.commit()
    click.echo("Done.")


if __name__ == "__main__":
    main()
