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
from typing import Callable, Optional

import click
from sqlalchemy import text

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


def fetch_from_queue(
    new_: bool,
    updated: bool,
    reannotate_ts: datetime.datetime | None,
    batch_size: int | None,
    loggerfn: Callable | None = None,
) -> list[dict]:
    """Fetch mbids to annotate from the "queue".

    Fetches mbids from the db according to the specified criteria. Returns a maximum of batch_size
    mbids, selected first from new mbids, then updated mbids, then old annotations to re-annotate.

    Args:
        new_: Whether to include new mbids that have not been annotated yet.
        updated: Whether to include mbids that have been updated since last annotation.
        reannotate_ts: Timestamp before which to re-annotate old annotations.
        batch_size: Maximum number of mbids to fetch.


    Returns:
        List of dicts with mbid and entity keys.
    """
    if not loggerfn:
        loggerfn = lambda _: None  # noqa: E731

    if new_:
        unannotated = get_unannotated_mbids()
        loggerfn(f"Found {len(unannotated)} unannotated mbid(s).")
    else:
        unannotated = []

    if updated:
        updated_mbids = get_updated_mbids()
        loggerfn(f"Found {len(updated_mbids)} updated mbid(s) to re-annotate.")
    else:
        updated_mbids = []

    if reannotate_ts is not None:
        old_annotations = get_very_old_annotations(reannotate_ts)
        loggerfn(f"Found {len(old_annotations)} very old mbid(s) to re-annotate.")
    else:
        old_annotations = []

    return select_topn_from_multilist_dicts(
        [unannotated, updated_mbids, old_annotations],
        N=batch_size if batch_size is not None else float("inf"),
        identity_key="mbid",
    )


def ingest_batch(
    batch: list[dict], report_interval: int = 25, loggerfn: Callable | None = None
) -> int:
    """Ingest a batch of mbids.

    Iterates through the batch, annotating each mbid and inserting/updating the
    MusicBrainzAnnotation table. Posts progress logs every report_interval items.

    Returns the number of mbids annotated. Maybe less than the batch in the case of skips due to
    timeouts.
    """
    if not loggerfn:
        loggerfn = lambda _: None  # noqa: E731

    # exit if there is nothing to do
    loggerfn(f"Annotating {len(batch)} total mbid(s).")
    if not batch:
        loggerfn("Nothing to do.")
        return 0

    # annotate and insert
    count_annotated = 0
    count_skipped = 0
    start_ts = datetime.datetime.now(datetime.timezone.utc)
    with get_session() as session:
        for args in batch:
            try:
                res = utils_.annotate_mbid(args["mbid"], args["entity"])
                count_annotated += 1
            except utils_.MusicBrainzTimeoutError:
                loggerfn(f"Timeout annotating mbid {args['mbid']}, {args['entity']}, skipping.")
                count_skipped += 1
                continue

            MusicBrainzAnnotation(
                mbid=args["mbid"],
                entity=args["entity"],
                payload_json=res,
                ts_utc=utils_.utcnow(),
            ).upsert(session=session)

            # log every report_interval annotations
            count_processed = count_annotated + count_skipped
            if count_processed % report_interval == 0:
                elapsed = datetime.datetime.now(datetime.timezone.utc) - start_ts
                remaining_timedelta = (elapsed / count_processed) * (len(batch) - count_processed)
                remaining_mins = remaining_timedelta.total_seconds() / 60

                p = count_processed / len(batch)

                loggerfn(
                    f"Processed {count_processed} ({p:.2%}) mbids. "
                    + f"Estimated remaining: {remaining_mins:0.2f} minutes."
                )

    # log the final stats
    count_processed = count_annotated + count_skipped
    total_elapsed = datetime.datetime.now(datetime.timezone.utc) - start_ts
    total_mins = total_elapsed.total_seconds() / 60
    anns_per_sec = count_processed / total_elapsed.total_seconds()
    loggerfn(
        f"Annotated of {count_annotated} mbid(s), "
        + f"skipped {count_skipped} mbid(s) "
        + f"in {total_mins:0.2f} minutes. "
        + f"Rate: {anns_per_sec:0.2f} anns/sec."
    )

    return count_annotated


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

    # get mbids to annotate
    batch = fetch_from_queue(
        new_=new_,
        updated=updated,
        reannotate_ts=before,
        batch_size=limit,
        loggerfn=click.echo,
    )
    ingest_batch(batch, loggerfn=click.echo)

    click.echo("Done.")


if __name__ == "__main__":
    main()
