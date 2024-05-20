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
from tqdm import tqdm

from . import utils_
from .db import MusicBrainzAnnotation, execute_sql_fetchall, get_session


def get_unannotated_mbids() -> list[dict]:
    """Get mbids that have not been annotated from the mbids table."""
    dbt_schema = os.environ["MOOMOO_DBT_SCHEMA"]
    sql = f"""
        select mbids.mbid as mbid, mbids.entity
        from {dbt_schema}.mbids
        left join {MusicBrainzAnnotation.table_name()} as src on mbids.mbid = src.mbid
        where src.mbid is null
          and mbids.entity = any(:entities)
    """
    return execute_sql_fetchall(sql, params=dict(entities=utils_.ENTITIES))


def get_re_annotate_mbids(before: datetime.datetime) -> list[dict]:
    """Get mbids that were annotated between from_dt and to_dt."""
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
    return execute_sql_fetchall(sql, params=dict(before=before, entities=utils_.ENTITIES))


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
    help="Upper bound on last-annotated-at timestamps to re-annotate.",
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
    to_ingest: list[dict] = []
    if new_:
        click.echo("Getting unannotated mbids...")
        unannotated = get_unannotated_mbids()
        to_ingest += unannotated
        click.echo(f"Found {len(unannotated)} unannotated mbid(s).")

    if before:
        click.echo(f"Getting mbids to re-annotate (before {before})...")
        reannotate = get_re_annotate_mbids(before)
        to_ingest += reannotate
        click.echo(f"Found {len(reannotate)} mbid(s) to re-annotate.")

    # exit if there is nothing to do
    click.echo(f"Found {len(to_ingest)} total mbid(s) to annotate.")
    if not to_ingest:
        click.echo("Nothing to do.")
        sys.exit(0)

    # limit if needed
    if limit and len(to_ingest) > limit:
        click.echo(f"Limiting to {limit} mbid(s) randomly.")
        to_ingest = random.sample(to_ingest, k=limit)

    # annotate and insert
    click.echo("Annotating...")
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
