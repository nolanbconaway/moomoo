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
import argparse
import datetime
import hashlib
import json
import os
import sys
import time

import psycopg2
import requests
from tqdm import tqdm

POSTGRES_DSN = os.environ["POSTGRES_DSN"]
API_KEY = os.environ["LASTFM_API_KEY"]


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


def get_with_retries(
    *args, retries: int = 3, delay: float = 3, timeout_: float = 2, **kwargs
) -> requests.Response:
    for i in range(retries):
        try:
            res = requests.get(*args, **kwargs)
            time.sleep(timeout_)
            res.raise_for_status()
            return res
        except requests.exceptions.HTTPError:
            if i == retries - 1:
                raise
            else:
                print("Connection error, retrying in {} seconds".format(delay))
                time.sleep(delay)


def get_listens_in_period(
    username: str, from_dt: datetime.datetime, to_dt: datetime.datetime
) -> list:
    """Get recent tracks for a user in a given period."""
    params = {
        "method": "user.getRecentTracks",
        "user": username,
        "api_key": API_KEY,
        "format": "json",
        "extended": "1",
        "from": int(from_dt.timestamp()),
        "to": int(to_dt.timestamp()),
        "limit": 200,
    }

    def get_page(page: int) -> dict:
        return get_with_retries(
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


def get_lastfm_user_registry_dt(username: str) -> datetime.datetime:
    """Get the date the user was registered on last.fm."""
    r = get_with_retries(
        "https://ws.audioscrobbler.com/2.0/",
        params=dict(
            method="user.getInfo", user=username, api_key=API_KEY, format="json"
        ),
    ).json()
    return utcfromunixtime(r["user"]["registered"]["unixtime"])


def get_db_last_listen(username: str, schema: str, table: str) -> datetime.datetime:
    """Get the last listen timestamp from the user in the db."""
    with psycopg2.connect(POSTGRES_DSN) as conn:
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
                listen_at_ts_utc=utcfromunixtime(listen_data["date"]["uts"]),
            ),
        )


def create_table(schema, table):
    print('Creating table "{}" in schema "{}"...'.format(table, schema))
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"drop table if exists {schema}.{table}".format(
                    schema=schema, table=table
                )
            )
            for sql in DDL:
                cur.execute(sql.format(schema=schema, table=table))
        conn.commit()


def utcfromisodate(iso_date: str) -> datetime.datetime:
    """Convert YYYY_MM_DD date to UTC datetime."""
    return datetime.datetime.strptime(iso_date, "%Y-%m-%d").replace(
        tzinfo=datetime.timezone.utc
    )


def utcfromunixtime(unixtime: int) -> datetime.datetime:
    """Convert unix timestamp to UTC datetime."""
    return datetime.datetime.utcfromtimestamp(int(unixtime)).replace(
        tzinfo=datetime.timezone.utc
    )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("username")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument(
        "--from",
        dest="from_dt",
        type=utcfromisodate,
        default=None,
        help=(
            "Date from which to start copying data (YYYY-MM-DD). "
            + "Defaults to the day the user was registered on last.fm."
        ),
    )
    parser.add_argument(
        "--to",
        dest="to_dt",
        type=utcfromisodate,
        default=datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc),
        help=(
            "Date after which to stop copying data (YYYY-MM-DD). "
            + "Defaults to utc now."
        ),
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Option to teardown and recreate the table",
    )
    parser.add_argument(
        "--since-last",
        action="store_true",
        help="Option to only copy data since the latest entry in the table",
    )
    return parser


if __name__ == "__main__":
    args = make_parser().parse_args()

    # defensive programming
    if args.since_last and args.from_dt is not None:
        raise ValueError("--since-last and --from are mutually exclusive")
    if args.since_last and args.create:
        raise ValueError("--since-last and --create are mutually exclusive")

    if args.create:
        create_table(args.schema, args.table)

    # get the from stamp
    if args.from_dt is not None:
        from_dt = args.from_dt
    elif args.since_last:
        from_dt = get_db_last_listen(args.username, args.schema, args.table)
        from_dt += datetime.timedelta(seconds=1)  # to make this left-exclusive
        if from_dt is None:
            raise RuntimeError("No listens found in database")
    else:
        from_dt = get_lastfm_user_registry_dt(args.username)

    if from_dt >= args.to_dt:
        raise ValueError(f"from date ({from_dt}) is after to date ({args.to_dt})")

    print(f"Getting {args.username} listens from {from_dt} to {args.to_dt}")
    data = get_listens_in_period(args.username, from_dt, args.to_dt)

    if not data:
        print("No listens found")
        sys.exit(0)

    with psycopg2.connect(POSTGRES_DSN) as conn:
        print(f"""Inserting {len(data)} listens into {args.schema}.{args.table}""")
        for row in tqdm(data):
            insert(
                conn, args.schema, args.table, username=args.username, listen_data=row
            )
        conn.commit()
