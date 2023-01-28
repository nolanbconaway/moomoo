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

from .. import utils_

DDL = [
    """
    create table {schema}.{table} (
        listen_md5 varchar(32) not null primary key
        , username text not null
        , json_data jsonb not null
        , listen_at_ts_utc timestamp with time zone not null
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
    """create index {schema}_{table}_username_idx on {schema}.{table} (username)""",
    """
    create index {schema}_{table}_listen_at_idx on {schema}.{table} (
        listen_at_ts_utc
    )""",
]


def get_listens_in_period(
    username: str, from_dt: datetime.datetime, to_dt: datetime.datetime
) -> list:
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


def get_db_last_listen(username: str, schema: str, table: str) -> datetime.datetime:
    """Get the last listen timestamp from the user in the db."""
    with utils_.pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                select max(listen_at_ts_utc) from {schema}.{table}
                where username = %(username)s
                """,
                dict(username=username),
            )
            return cur.fetchone()[0]


def listen_hash(username: str, data: dict) -> str:
    """Get the hash of a listen using only immutable fields."""
    immutable = [username, data["recording_msid"], data["listened_at"]]
    return hashlib.md5(json.dumps(immutable).encode("utf-8")).hexdigest()


def check_user_in_table(schema: str, table: str, username: str) -> bool:
    with utils_.pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"select 1 from {schema}.{table} where username = %(username)s limit 1",
                dict(username=username),
            )
            res = cur.fetchall()
    return any(res)


def insert(conn, schema: str, table: str, username: str, listen_data: dict):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {schema}.{table} (listen_md5, username, json_data, listen_at_ts_utc)
            values (%(listen_md5)s, %(username)s, %(json_data)s, %(listen_at_ts_utc)s)
            on conflict (listen_md5) do update
            set username = excluded.username
              , json_data = excluded.json_data
              , listen_at_ts_utc = excluded.listen_at_ts_utc
            """,
            dict(
                listen_md5=listen_hash(username, listen_data),
                username=username,
                json_data=json.dumps(listen_data),
                listen_at_ts_utc=utils_.utcfromunixtime(listen_data["listened_at"]),
            ),
        )


def run_ingest(
    username: str,
    table: str,
    schema: str,
    from_dt: datetime.datetime,
    to_dt: datetime.datetime,
):
    """Ingest data from listenbrainz."""
    if from_dt >= to_dt:
        raise ValueError(f"from date ({from_dt}) is after to date ({to_dt})")

    print(f"Getting {username} listens from {from_dt} to {to_dt}")
    data = get_listens_in_period(username, from_dt, to_dt)

    if not data:
        print("No listens found")
        return

    with utils_.pg_connect() as conn:
        print(f"""Inserting {len(data)} listens into {schema}.{table}""")
        for row in tqdm(data):
            insert(conn, schema, table, username=username, listen_data=row)
        conn.commit()


@click.command()
@click.argument("username")
@click.option("--table", required=True)
@click.option("--schema", required=True)
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
    "--create", is_flag=True, help="Option to teardown and recreate the table"
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
    table: str,
    schema: str,
    create: bool,
    since_last: bool,
    from_dt: Optional[datetime.datetime],
    to_dt: datetime.datetime,
    buffer_days: int,
):
    """Query the ListenBrainz listens api and store it in the db.

    This script is meant to be run periodically to update the db with recent tracks. It uses
    an md5 hash of the (listen user, timestamp, recording_msid) to uniquely identify a
    listen event. A postgres on conflict clause is used to update the listen if it already
    exists and ensure uniqueness.
    """
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    if since_last and from_dt:
        click.echo(
            "--since-last and --from are mutually exclusive. Use --since-last instead."
        )
        sys.exit(1)

    # get from date
    if since_last:
        if not check_user_in_table(schema=schema, table=table, username=username):
            click.echo(
                f"No data found for {username} in {schema}.{table}. "
                + "Cannot use --since-last"
            )
            sys.exit(1)
        from_dt = get_db_last_listen(schema=schema, table=table, username=username)
        from_dt -= datetime.timedelta(days=buffer_days)
    elif not from_dt:
        click.echo("Must specify either --since-last or --from")
        sys.exit(1)

    run_ingest(
        username=username, table=table, schema=schema, from_dt=from_dt, to_dt=to_dt
    )


if __name__ == "__main__":
    main()
