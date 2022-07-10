"""Query the Last.FM recent tracks api and store it in the db.

This script is meant to be run periodically to update the db with recent tracks. It uses
a sha256 hash of the (listen user, artist name, track name, timestamp) to uniquely 
identify a listen. event. A postgres on conflict clause is used to update the listen
if it already exists and ensure uniqueness.

API Docs: https://www.last.fm/api/show/user.getRecentTracks

Sample Payload:

    {
        "recenttracks": {
            "track": [
                {
                    "artist": {
                        "url": "...",
                        "name": "Chuck Person",
                        "image": [{ "size": "small", "#text": "..." }, ... ],
                        "mbid": ""
                    },
                    "date": {"uts": "1654371703", "#text": "04 Jun 2022, 19:41"},
                    "mbid": "",
                    "name": "...",
                    "image": [{ "size": "small", "#text": "..." }, ... ],
                    "url": "...",
                    "streamable": "0",
                    "album": { "mbid": "...", #text": "..." },
                    "loved": "0"
                }
            ],
            "@attr": {
                "user": "...",
                "totalPages": "5",
                "page": "3",
                "perPage": "1",
                "total": "5"
            }
        }
    }
"""
import datetime
import hashlib
import json
import os
import sys

import click
from tqdm import tqdm

from . import utils_

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
    """create index {schema}_{table}_listen_at_idx on {schema}.{table} (listen_at_ts_utc)""",
]


def get_listens_in_period(
    username: str,
    from_dt: datetime.datetime,
    to_dt: datetime.datetime,
    api_key: str = None,
) -> list:
    """Get recent tracks for a user in a given period."""
    params = {
        "method": "user.getRecentTracks",
        "user": username,
        "api_key": api_key or os.environ["LASTFM_API_KEY"],
        "format": "json",
        "extended": "1",
        "from": int(from_dt.timestamp()),
        "to": int(to_dt.timestamp()),
        "limit": 200,
    }

    def get_page(page: int) -> dict:
        return utils_.get_with_retries(
            "https://ws.audioscrobbler.com/2.0/", params=dict(page=page, **params)
        ).json()

    pages = [get_page(1)]

    # exit early if there are no listens
    if int(pages[0]["recenttracks"]["@attr"]["total"]) == 0:
        print('User "{}" has no listens in period.'.format(username))
        return []

    # get all pages if any more than one
    total_pages = int(pages[0]["recenttracks"]["@attr"]["totalPages"])
    if total_pages > 1:
        for page in tqdm(range(2, total_pages + 1)):
            pages.append(get_page(page))

    # flatten the list of pages
    res = [
        item
        for page in pages
        for item in page["recenttracks"]["track"]
        if "date" in item  # current play does not have a date
    ]

    return res


def get_lastfm_user_registry_dt(
    username: str, api_key: str = None
) -> datetime.datetime:
    """Get the date the user was registered on last.fm."""
    r = utils_.get_with_retries(
        "https://ws.audioscrobbler.com/2.0/",
        params=dict(
            method="user.getInfo",
            user=username,
            api_key=api_key or os.environ["LASTFM_API_KEY"],
            format="json",
        ),
    ).json()
    return utils_.utcfromunixtime(r["user"]["registered"]["unixtime"])


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
    immutable = [username, data["artist"]["name"], data["date"]["uts"], data["name"]]
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
                listen_at_ts_utc=utils_.utcfromunixtime(listen_data["date"]["uts"]),
            ),
        )


def run_ingest(
    username: str,
    table: str,
    schema: str,
    from_dt: datetime.datetime,
    to_dt: datetime.datetime,
):
    """Ingest data from last.fm."""
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
    "--since-register",
    "since",
    flag_value="register",
    help="Option to copy all data since the user was registered.",
)
@click.option(
    "--since-last",
    "since",
    flag_value="last",
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
def main(username, table, schema, create, since, from_dt, to_dt):
    """Query the Last.FM recent tracks api and store it in the db.

    This script is meant to be run periodically to update the db with recent tracks. It
    uses an md5 hash of the (user, artist name, track name, timestamp) to uniquely
    identify a listen. event. A postgres on conflict clause is used to update the listen
    if it already exists and ensure uniqueness.
    """
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    if since and from_dt:
        click.echo(
            "--since-* and --from are mutually exclusive. Use --since-* instead."
        )
        sys.exit(1)

    # get from date
    if since == "last":
        if not check_user_in_table(schema=schema, table=table, username=username):
            click.echo(
                f"No data found for {username} in {schema}.{table}. "
                + "Cannot use --since-last"
            )
            sys.exit(1)
        from_dt = get_db_last_listen(schema=schema, table=table, username=username)
    elif since == "register":
        from_dt = get_lastfm_user_registry_dt(username)

    run_ingest(
        username=username, table=table, schema=schema, from_dt=from_dt, to_dt=to_dt
    )


if __name__ == "__main__":
    main()
