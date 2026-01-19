"""Enrich the database with metadata about mbids with data from MusicBrainz.

Requires that the dbt models have been run to populate the mbids table, as we need to
collect all mbids from listens, local files, etc.

This tool has logic to detect new mbids that have not been annotated yet, as well as
mbids that have been annotated but may need refreshing. The latter is useful for mbids
that were enriched long ago, but may have new data available in MusicBrainz.

In practice, it takes roughly ~1s to annotate a single mbid. Use this with the --limit
option to limit the total run time.
"""

import dataclasses
import datetime
import os
from collections import deque
from typing import Callable
from uuid import UUID

import click
from sqlalchemy import text
from sqlalchemy.orm import Session

from . import utils_
from .db import (
    MusicBrainzAnnotation,
    MusicBrainzDataDump,
    MusicBrainzDataDumpRecord,
    execute_sql_fetchall,
    get_session,
)


@dataclasses.dataclass
class Mbid:
    """Container for mbid and entity type."""

    mbid: UUID
    entity: str

    @classmethod
    def from_sql_rows(cls, rows: list[dict]) -> list["Mbid"]:
        """Create list of Mbid from sql rows."""
        to_uuid = lambda val: UUID(val) if isinstance(val, str) else val
        return [cls(mbid=to_uuid(row["mbid"]), entity=row["entity"]) for row in rows]

    def to_dict(self) -> dict:
        """Convert to dict."""
        return dict(mbid=self.mbid, entity=self.entity)


def get_unannotated_mbids() -> list[Mbid]:
    """Get mbids that have not been annotated from the mbids table."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid as mbid, mbids.entity
        from {dbt_schema}.mbids
        left join {MusicBrainzAnnotation.table_name()} as src on mbids.mbid = src.mbid
        where src.mbid is null
          and mbids.entity = any(:entities)
    """
    return Mbid.from_sql_rows(execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES)))


def get_updated_mbids() -> list[Mbid]:
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

    select mbid, entity
    from {MusicBrainzAnnotation.table_name()} as annotations
    inner join last_update using (entity, mbid)
    -- the dumps are hourly, so the record could have been updated anytime within the hour. i think
    -- the dump timestamp corresponds to the start of the period covered by the dump, so to be safe
    -- assume that all records were updated at the very last minute and add a 20min buffer.
    where annotations.ts_utc < last_update.ts + interval '20 minutes'
    """
    return Mbid.from_sql_rows(execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES)))


def get_very_old_annotations(before: datetime.datetime) -> list[Mbid]:
    """Get mbids that were annotated before a given timestamp."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid, mbids.entity
        from {dbt_schema}.mbids
        inner join {MusicBrainzAnnotation.table_name()} as src
            on mbids.mbid::varchar = src.mbid::varchar
        where src.ts_utc < :before
            and mbids.entity = any(:entities)
        order by src.ts_utc
    """
    return Mbid.from_sql_rows(
        execute_sql_fetchall(sql, params=dict(before=before, entities=utils_.ENTITIES))
    )


def drop_dangling_annotations(loggerfn: Callable | None = None) -> int:
    """Run a delete statement for any historical failed annotations that have no entry in mbids."""
    if not loggerfn:
        loggerfn = lambda _: None
    loggerfn("Dropping dangling annotations...")
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
        loggerfn(f"Deleted {deleted} dangling annotations.")
        session.commit()
    return deleted


def fetch_queue(
    new_: bool,
    updated: bool,
    reannotate_ts: datetime.datetime | None,
    limit: int | None,
    loggerfn: Callable | None = None,
) -> deque[Mbid]:
    """Fetch mbids to annotate from the "queue".

    Fetches mbids from the db according to the specified criteria. Returns a maximum of batch_size
    mbids, selected first from new mbids, then updated mbids, then old annotations to re-annotate.

    Args:
        new_: Whether to include new mbids that have not been annotated yet.
        updated: Whether to include mbids that have been updated since last annotation.
        reannotate_ts: Timestamp before which to re-annotate old annotations.
        limit: Maximum number of mbids to fetch.


    Returns:
        List of dicts with mbid and entity keys.
    """
    if not loggerfn:
        loggerfn = lambda _: None

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

    result: list[Mbid] = utils_.topn_from_multilists(
        [unannotated, updated_mbids, old_annotations],
        N=limit if limit is not None else float("inf"),
        identity_fn=lambda x: x.mbid,
    )
    return deque(result)


def list_dependents(payload: dict) -> list[Mbid]:
    """List dependent mbids from an annotation payload.

    The payload is expected to be the dict returned by annotate_mbid.
    """
    # no dependents if not successful
    if not payload.get("_success"):
        return []

    # no dependents if no data
    if not payload.get("data"):
        return []

    # no dependents if no args
    if not payload.get("_args"):
        return []

    # get entity type
    entity = payload["_args"].get("entity")
    if entity is None:
        return []

    data = payload["data"]

    # if artist, list all releases
    if entity == "artist":
        release_list = data.get("artist", {}).get("release-list", [])
        return [Mbid(mbid=UUID(release["id"]), entity="release") for release in release_list]

    # if release, list the release group
    if entity == "release":
        release_group = data.get("release", {}).get("release-group", {})
        if release_group:
            return [Mbid(mbid=UUID(release_group["id"]), entity="release-group")]

    return []


def filter_dependent_mbids(dependents: list[Mbid]) -> list[Mbid]:
    """Filter dependent mbids to only those that do not already have annotations."""
    if not dependents:
        return []

    with get_session() as session:
        # populate a temporary table with the dependent mbids
        tablename = f"tmp_dependents_{int(datetime.datetime.now().timestamp() * 1000)}"
        sql = f"create temp table {tablename} (mbid uuid primary key, entity varchar)"
        session.execute(text(sql))
        session.execute(
            text(f"insert into {tablename} (mbid, entity) values (:mbid, :entity)"),
            params=[dep.to_dict() for dep in dependents],
        )

        # select only those mbids that do not already have annotations
        sql = f"""
            select distinct dependents.mbid, dependents.entity
            from {tablename} as dependents 
            left join {MusicBrainzAnnotation.table_name()} as src on src.mbid = dependents.mbid
            where src.mbid is null
              and dependents.entity = any(:entities)
            order by dependents.mbid
        """
        res = Mbid.from_sql_rows(
            execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES), session=session)
        )

        # drop the temporary table. should be automatic on session close, but just in case.
        session.execute(text(f"drop table if exists {tablename}"))

    return res


def ingest_mbid(mbid: UUID, entity: str, session=Session) -> dict:
    """Annotate a single mbid and upsert the MusicBrainzAnnotation table.

    Returns the annotation payload. Extracted to this function to make try-except handling easier.
    """
    payload = utils_.annotate_mbid(mbid=str(mbid), entity=entity)
    MusicBrainzAnnotation(
        mbid=mbid,
        entity=entity,
        payload_json=payload,
        ts_utc=utils_.utcnow(),
    ).upsert(session=session)
    return payload


def annotate_and_upsert(
    queue: deque[Mbid],
    ingest_dependents: bool = False,
    report_interval: int = 25,
    loggerfn: Callable | None = None,
) -> tuple[int, int]:
    """Ingest a queue of mbids.

    Iterates through the queue, annotating each mbid and inserting/updating the
    MusicBrainzAnnotation table. Posts progress logs every report_interval items.

    Returns a tuple of (count_annotated, count_skipped). The counts may be greater than the size
    of the input batch if ingest_dependents is True, as dependents are also annotated.
    """
    if not loggerfn:
        loggerfn = lambda _: None

    # exit if there is nothing to do
    if not queue:
        loggerfn("Nothing to do.")
        return 0, 0

    loggerfn(f"Annotating {len(queue)} total mbid(s)...")

    # annotate and insert
    queue_length = len(queue)
    count_annotated = 0
    count_skipped = 0
    dependents = []
    start_ts = datetime.datetime.now(datetime.timezone.utc)
    with get_session() as session:
        while queue:
            mbid = queue.popleft()
            try:
                res = ingest_mbid(mbid=mbid.mbid, entity=mbid.entity, session=session)
                count_annotated += 1
                dependents += list_dependents(res)
            except utils_.MusicBrainzTimeoutError:
                loggerfn(f"Timeout annotating mbid {mbid.mbid}, {mbid.entity}, skipping.")
                count_skipped += 1
                continue

            # log every report_interval annotations
            count_processed = count_annotated + count_skipped
            if count_processed % report_interval == 0:
                elapsed = datetime.datetime.now(datetime.timezone.utc) - start_ts
                remaining_timedelta = (elapsed / count_processed) * (queue_length - count_processed)
                remaining_mins = remaining_timedelta.total_seconds() / 60
                p = count_processed / queue_length
                loggerfn(
                    f"Processed {count_processed}/{queue_length} ({p:.2%}) mbids. "
                    + f"Estimated remaining: {remaining_mins:0.2f} minutes."
                )

    # log the final stats
    count_processed = count_annotated + count_skipped
    total_elapsed = datetime.datetime.now(datetime.timezone.utc) - start_ts
    total_mins = total_elapsed.total_seconds() / 60
    anns_per_sec = count_processed / total_elapsed.total_seconds()
    loggerfn(
        f"Annotated {count_annotated} mbid(s), "
        + f"skipped {count_skipped} mbid(s) "
        + f"in {total_mins:0.2f} minutes. "
        + f"Rate: {anns_per_sec:0.2f} anns/sec."
    )

    if ingest_dependents:
        # unique-ify dependents and filter to only those that need annotation
        dependents: list[Mbid] = list(utils_.unique_by(dependents, key=lambda x: x.mbid))
        dependents = filter_dependent_mbids(dependents)
        loggerfn(f"Found {len(dependents)} dependent mbid(s) to annotate.")

        # recursively annotate
        dep_count_annotated, dep_count_skipped = annotate_and_upsert(
            deque(dependents),
            report_interval=report_interval,
            loggerfn=loggerfn,
            ingest_dependents=True,
        )
        count_annotated += dep_count_annotated
        count_skipped += dep_count_skipped
    return count_annotated, count_skipped


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
@click.option(
    "--dependents",
    is_flag=True,
    default=False,
    help="Option to also ingest dependents. If set, annotations will likely exceed --limit.",
)
def main(
    new_: bool,
    updated: bool,
    before: datetime.datetime | None,
    limit: int | None,
    drop: bool,
    dependents: bool,
):
    """Run the main CLI."""
    # drop dangling annotations
    if drop:
        drop_dangling_annotations(loggerfn=click.echo)

    # get mbids to annotate
    queue = fetch_queue(
        new_=new_,
        updated=updated,
        reannotate_ts=before,
        limit=limit,
        loggerfn=click.echo,
    )

    annotate_and_upsert(queue, loggerfn=click.echo, ingest_dependents=dependents)
    click.echo("Done.")


if __name__ == "__main__":
    main()
