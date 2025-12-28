"""Enrich the database with metadata about mbids with data from MusicBrainz.

Requires that the dbt models have been run to populate the mbids table, as we need to
collect all mbids from listens, local files, etc.

This tool has logic to detect new mbids that have not been annotated yet, as well as
mbids that have been annotated but may need refreshing. The latter is useful for mbids
that were enriched long ago, but may have new data available in MusicBrainz.

In practice, it takes roughly ~1s to annotate a single mbid. Use this with the --limit
option to limit the total run time.
"""

import datetime
import os
import random
import sys
from typing import Optional

import click
from sqlalchemy import text
from tqdm import tqdm

from . import utils_
from .db import (
    MusicBrainzAnnotation,
    MusicBrainzDataDump,
    MusicBrainzDataDumpRecord,
    execute_sql_fetchall,
    get_session,
)


def get_unannotated_mbids() -> list[dict]:
    """Get mbids that have not been annotated from the mbids table."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid as mbid, mbids.entity, '0 new' as source
        from {dbt_schema}.mbids
        left join {MusicBrainzAnnotation.table_name()} as src on mbids.mbid = src.mbid
        where src.mbid is null
          and mbids.entity = any(:entities)
    """
    return execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES))


def get_updated_mbids() -> list[dict]:
    """Get mbids that have updates in the musicbrainz database since they were last annotated."""
    sql = f"""
    with updates as (
        -- the actual record updated
        select
            dumps.entity
            , records.mbid
            , max(dumps.dump_timestamp) as ts
        from {MusicBrainzDataDump.table_name()} as dumps
        inner join {MusicBrainzDataDumpRecord.table_name()} as records using (slug)
        where dumps.entity = any(:entities)
        group by 1, 2

        union all

        -- each of the containers updated
        select
            container ->> 'entity' as entity
            , (container ->> 'mbid')::uuid as mbid
            , max(dumps.dump_timestamp) as ts
        from {MusicBrainzDataDump.table_name()} as dumps
        inner join {MusicBrainzDataDumpRecord.table_name()} as records using (slug)
          , jsonb_array_elements(records.json_data -> 'containers') as container

        where records.json_data -> 'containers' is not null
          and container ->> 'mbid' is not null
          and container ->> 'entity' is not null
          and dumps.entity = any(:entities)
          and container ->> 'entity' = any(:entities)

        group by 1, 2
    )

    -- dedupe to get the last update per (entity, mbid)
    , last_update as (
        select entity, mbid, max(ts) as ts
        from updates
        group by 1, 2
    )

    select mbid, entity, '1 update' as source
    from {MusicBrainzAnnotation.table_name()} as annotations
    inner join last_update using (entity, mbid)
    -- the dumps are hourly, so the record could have been updated anytime within the hour. i think
    -- the dump timestamp corresponds to the start of the period covered by the dump, so to be safe
    -- assume that all records were updated at the very last minute and add a 20min buffer.
    where annotations.ts_utc < last_update.ts + interval '20 minutes'
    """
    return execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES))


def get_very_old_annotations(before: datetime.datetime) -> list[dict]:
    """Get mbids that were annotated before a given timestamp."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid, mbids.entity, '2 old' as source
        from {dbt_schema}.mbids
        inner join {MusicBrainzAnnotation.table_name()} as src
            on mbids.mbid::varchar = src.mbid::varchar
        where src.ts_utc < :before
            and mbids.entity = any(:entities)
        order by src.ts_utc
    """
    return execute_sql_fetchall(sql, params=dict(before=before, entities=utils_.ENTITIES))


def drop_dangling_annotations():
    """Run a delete statement for any historical failed annotations that have no entry in mbids."""
    click.echo("Dropping dangling annotations...")
    with get_session() as session:
        sql = f"""
            delete from {MusicBrainzAnnotation.table_name()} as src
            where mbid in (
                select src.mbid
                from {MusicBrainzAnnotation.table_name()} as src
                left join {os.environ["MOOMOO_DBT_SCHEMA"]}.mbids as mbids
                    on mbids.mbid::varchar = src.mbid::varchar
                where mbids.mbid is null
                    and not coalesce((payload_json ->> '_success')::bool, true)
                    and ts_utc < now() - interval '1 month'
            )
        """
        res = session.execute(text(sql))
        deleted = res.rowcount
        click.echo(f"Deleted {deleted} dangling annotations.")
        session.commit()
    return deleted


def select_topn_from_multilist_dicts(
    lists: list[list[dict]], N: int, identity_key: str
) -> list[dict]:
    """Chatgpt to deduplicate and select top N from multiple lists of dicts.

    Selects up to N unique items from multiple lists of dictionaries based on a specified identity
    key. Grabs items from the input lists in order, ensuring no duplicates based on the identity
    key.
    """
    seen = set()
    output = []

    for lst in lists:
        random.shuffle(lst)
        for item in lst:
            k = item[identity_key]  # the dedupe key
            if k not in seen:
                seen.add(k)
                output.append(item)
                if len(output) == N:
                    return output

    return output


@click.command(help=__doc__)
@click.option(
    "--new",
    "new_",
    is_flag=True,
    help="Option to detect new mbids that have not been annotated yet.",
)
@click.option(
    "--updated",
    "updated",
    is_flag=True,
    help="Option to detect mbids that have been updated since they were last annotated.",
)
@click.option(
    "--before",
    "before",
    type=utils_.utcfromisodate,
    help="Upper bound on last-annotated-at timestamps to re-annotate.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    help="Limit the number of mbids to annotate.",
    default=None,
)
@click.option(
    "--drop/--no-drop",
    type=bool,
    is_flag=True,
    help="Option to first drop any dangling annotations.",
    default=True,
)
def main(
    new_: bool,
    updated: bool,
    before: Optional[datetime.datetime],
    limit: Optional[int],
    drop: bool,
):
    """Run the main CLI."""
    # drop dangling annotations
    if drop:
        drop_dangling_annotations()

    # get  of mbids to annotate
    if new_:
        unannotated = get_unannotated_mbids()
        click.echo(f"Found {len(unannotated)} unannotated mbid(s).")
    else:
        unannotated = []

    if before:
        old_annotations = get_very_old_annotations(before)
        click.echo(f"Found {len(old_annotations)} very old mbid(s) to re-annotate.")
    else:
        old_annotations = []

    if updated:
        updated_mbids = get_updated_mbids()
        click.echo(f"Found {len(updated_mbids)} updated mbid(s) to re-annotate.")
    else:
        updated_mbids = []

    limit = limit or float("inf")  # use all if no limit specified
    to_ingest = select_topn_from_multilist_dicts(
        [unannotated, updated_mbids, old_annotations],
        N=limit,
        identity_key="mbid",
    )
    # exit if there is nothing to do
    click.echo(f"Annotating {len(to_ingest)} total mbid(s).")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # annotate and insert
    annotated = utils_.annotate_mbid_batch(to_ingest)
    with get_session() as session:
        for args, res in tqdm(zip(to_ingest, annotated), disable=None, total=len(to_ingest)):
            MusicBrainzAnnotation(
                mbid=args["mbid"],
                entity=args["entity"],
                payload_json=res,
                ts_utc=utils_.utcnow(),
            ).upsert(session=session)

    click.echo("Done.")


if __name__ == "__main__":
    main()
