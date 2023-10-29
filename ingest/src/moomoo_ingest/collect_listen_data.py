"""Query the ListenBrainz listens api and store it in the db.

This script is meant to be run periodically to update the db with recent tracks. It uses
an md5 hash of the (listen user, timestamp, recording_msid) to uniquely identify a 
listen event. A postgres on conflict clause is used to update the listen if it already
exists and ensure uniqueness.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/core.html#get--1-user-(user_name)-listens
"""
import datetime
import hashlib
import json
import sys
from typing import Optional

import click
from pylistenbrainz import ListenBrainz
from pylistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
from tqdm import tqdm

from . import utils_
from .db import ListenBrainzListen, get_session


def get_listens_in_period(
    username: str, from_dt: datetime.datetime, to_dt: datetime.datetime
) -> list[dict]:
    """Get recent tracks for a user in a given period."""
    client = ListenBrainz()
    endpoint = "/1/user/{username}/listens".format(username=username)
    from_ts = int(from_dt.timestamp())
    to_ts = int(to_dt.timestamp())

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5),
        retry=retry_if_exception_type(ListenBrainzAPIException),
    )
    def get(lb: int, ub: int) -> dict:
        ub_dt = utils_.utcfromunixtime(ub).isoformat()
        lb_dt = utils_.utcfromunixtime(lb).isoformat()
        click.echo(f"Getting {username} page from {lb_dt} to {ub_dt}.")
        params = {"min_ts": lb, "max_ts": ub, "count": 100}
        return client._get(endpoint, params=params)["payload"]

    # get the first page
    payload = get(from_ts, to_ts)
    listens = payload["listens"]

    # end whenever we get less than 100 listens. we set a max 100 per page, so any
    # less than that means we are at the end.
    while payload["count"] == 100 and from_ts < to_ts:
        from_ts = max([i["listened_at"] + 1 for i in payload["listens"]])
        payload = get(from_ts, to_ts)
        listens += payload["listens"]

    return listens


def listen_hash(username: str, data: dict) -> str:
    """Get the hash of a listen using only immutable fields."""
    immutable = [username, data["recording_msid"], data["listened_at"]]
    return hashlib.md5(json.dumps(immutable).encode("utf-8")).hexdigest()


def run_ingest(username: str, from_dt: datetime.datetime, to_dt: datetime.datetime):
    """Ingest data from listenbrainz."""
    if from_dt >= to_dt:
        raise ValueError("from_dt must be before to_dt.")

    click.echo(f"Getting {username} listens from {from_dt} to {to_dt}")
    data = get_listens_in_period(username, from_dt, to_dt)

    if not data:
        click.echo("No listens found")
        return

    with get_session() as session:
        click.echo(f"Inserting {len(data)} listen(s).")
        for row in tqdm(data):
            listen = ListenBrainzListen(
                listen_md5=listen_hash(username, row),
                username=username,
                json_data=row,
                listen_at_ts_utc=utils_.utcfromunixtime(row["listened_at"]),
                insert_ts_utc=utils_.utcnow(),
            )
            listen.upsert(
                session=session,
                update_cols=["json_data", "listen_at_ts_utc", "insert_ts_utc"],
            )


@click.command()
@click.argument("username")
@click.option(
    "--since-last",
    is_flag=True,
    help="Option to only copy data since the latest entry in the table",
)
@click.option(
    "--from",
    "from_dt",
    type=utils_.utcfromisodate,
    help="Start date in iso-format. Irrelevant if --since-* is used.",
)
@click.option(
    "--to",
    "to_dt",
    type=utils_.utcfromisodate,
    default=datetime.datetime.utcnow().isoformat(),
    help="End date in iso-format. Defaults to now.",
)
@click.option(
    "--buffer-days",
    type=int,
    default=0,
    help=(
        "Number of days to buffer from the last listen, to catch late arriving data."
        + " Useed with --since-last. Default 0."
    ),
)
def main(
    username: str,
    since_last: bool,
    from_dt: Optional[datetime.datetime],
    to_dt: datetime.datetime,
    buffer_days: int,
):
    """Query the ListenBrainz listens api and store it in the db.

    This script is meant to be run periodically to update the db with recent tracks. It
    uses an md5 hash of the (listen user, timestamp, recording_msid) to uniquely
    identify a listen event. A postgres on conflict clause is used to update the listen
    if it already exists and ensure uniqueness.
    """
    if since_last and from_dt:
        click.echo("--since-last and --from are mutually exclusive.")
        sys.exit(1)

    if buffer_days and not since_last:
        click.echo("warn: --buffer-days is only used with --since-last.")

    # get from date
    if since_last:
        from_dt = ListenBrainzListen.last_listen_for_user(username)
        if from_dt is None:
            click.echo(f"No data found for {username}. Cannot use --since-last.")
            sys.exit(1)
        from_dt -= datetime.timedelta(days=buffer_days)
    elif not from_dt:
        click.echo("Must specify either --since-last or --from")
        sys.exit(1)

    run_ingest(username=username, from_dt=from_dt, to_dt=to_dt)
    click.echo("Done.")


if __name__ == "__main__":
    main()
