"""Annotate known mbids with existing data from MusicBrainz.

The status of an mbid falls into three categories:

1. New unannotated data.
2. Old data that is not annotated because the process failed earlier.
3. Old data that is already annotated.

We want to periodically re-check old data in case annotations have arrived in the db. So
we need to keep track of when we checked as well as the status of the check.
"""
import datetime
import json
import random
import sys
from typing import List, Optional
from uuid import UUID

import click
from tqdm import tqdm

from .. import utils_
from . import mbz_utils

DDL = [
    """
    create table {schema}.{table} (
        mbid uuid not null primary key,
        entity varchar not null,
        ts_utc timestamp with time zone default current_timestamp not null,
        payload_json jsonb
    )
    """,
    """create index {schema}_{table}_entity_idx on {schema}.{table} (entity)""",
    """create index {schema}_{table}_ingest_ts_idx on {schema}.{table} (ts_utc)""",
]


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def insert(conn, schema: str, table: str, mbid: str, entity: str, payload: dict):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {schema}.{table} (mbid, entity, payload_json)
            values (%(mbid)s, %(entity)s, %(payload_json)s)
            on conflict (mbid) do update
            set entity = excluded.entity
              , ts_utc = current_timestamp
              , payload_json = excluded.payload_json
            """,
            dict(
                mbid=mbid,
                entity=entity,
                payload_json=json.dumps(payload, cls=UUIDEncoder),
            ),
        )
        conn.commit()


def get_unannotated_mbids(
    annotate_schema: str, annotate_table: str, dbt_schema: str
) -> List[dict]:
    """Get mbids that have not been annotated from the mbids table."""
    sql = f"""
        select mbids.mbid as mbid, mbids.entity
        from {dbt_schema}.mbids
        left join {annotate_schema}.{annotate_table} as src on mbids.mbid = src.mbid
        where src.mbid is null
    """
    return utils_.execute_sql_fetchall(sql)


def get_re_annotate_mbids(
    annotate_schema: str,
    annotate_table: str,
    dbt_schema: str,
    from_dt: datetime.datetime,
    to_dt: datetime.datetime,
) -> List[dict]:
    """Get mbids that were annotated between from_dt and to_dt."""
    sql = f"""
        select mbids.mbid, mbids.entity
        from {dbt_schema}.mbids
        left join {annotate_schema}.{annotate_table} as src
            on mbids.mbid::varchar = src.mbid::varchar
        where src.ts_utc >= %(lb_utc)s and src.ts_utc < %(ub_utc)s
        order by src.ts_utc
    """
    return utils_.execute_sql_fetchall(sql, params=dict(lb_utc=from_dt, ub_utc=to_dt))


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
    "--find-new",
    is_flag=True,
    help="Option to detect new mbids that have not been annotated yet.",
)
@click.option(
    "--re-annotate-lb",
    "from_dt",
    type=utils_.utcfromisodate,
    help="Lower bound on last-annotated-at timestamps to re-annotate.",
)
@click.option(
    "--re-annotate-ub",
    "to_dt",
    type=utils_.utcfromisodate,
    help="Upper bound on last-annotated-at timestamps to re-annotate.",
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
    find_new: bool,
    from_dt: Optional[datetime.datetime],
    to_dt: Optional[datetime.datetime],
    limit: Optional[int],
):
    """Run the main CLI."""
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    if bool(from_dt) != bool(to_dt):
        click.echo("--re-annotate-lb and --re-annotate-ub must be used together.")
        sys.exit(1)
    elif bool(from_dt) and from_dt >= to_dt:
        click.echo("--re-annotate-lb must be before --re-annotate-ub.")
        sys.exit(1)

    # get list of mbids to annotate
    to_ingest: List[dict] = []
    if find_new:
        click.echo("Getting unannotated mbids...")
        to_ingest += get_unannotated_mbids(
            annotate_schema=schema, annotate_table=table, dbt_schema=dbt_schema
        )
    if bool(from_dt):
        click.echo(f"Getting mbids to re-annotate (from {from_dt} to {to_dt})...")
        to_ingest += get_re_annotate_mbids(
            annotate_schema=schema,
            annotate_table=table,
            dbt_schema=dbt_schema,
            from_dt=from_dt,
            to_dt=to_dt,
        )

    # exit if there is nothing to do
    click.echo(f"Found {len(to_ingest)} mbids to annotate.")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # limit if needed
    if limit and len(to_ingest) > limit:
        click.echo(f"Limiting to {limit} mbids randomly.")
        to_ingest = random.choices(to_ingest, k=limit)

    # annotate and insert
    click.echo("annotating...")
    annotated = mbz_utils.annotate_mbid_batch(to_ingest)
    with utils_.pg_connect() as conn:
        for args, res in tqdm(
            zip(to_ingest, annotated), disable=None, total=len(to_ingest)
        ):
            insert(
                conn=conn,
                schema=schema,
                table=table,
                mbid=args["mbid"],
                entity=args["entity"],
                payload=res,
            )
    click.echo("Done.")


if __name__ == "__main__":
    main()
