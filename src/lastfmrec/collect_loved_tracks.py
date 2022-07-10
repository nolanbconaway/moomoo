"""Query the Last.FM loved tracks api and store it in the db.

This script is meant to be run periodically to update the db with new loves. It uses
a md5 hash of the (user, artist name, track name, timestamp) to uniquely 
identify a love. event. A postgres on conflict clause is used to update the love
if it already exists and ensure uniqueness.

This is somewhat annoying as you cannot query based on a timestamp, so we have to
roll back N pages unless we want to ingest it all. One COULD set up an endpoint to get 
the last known love timestamp and roll pages until there is a love before then, but the
numerosity of these data are not large enough to justify the extra effort.

API Docs: https://www.last.fm/api/show/user.getLovedTracks

Sample Payload:

{
    "lovedtracks": {
        "track": [
            {
                "artist": {
                    "url": "https://www.last.fm/music/Yuji+Toriyama",
                    "name": "Yuji Toriyama",
                    "mbid": ""
                },
                "date": {"uts": "1657288699", "#text": "08 Jul 2022, 13:58"},
                "mbid": "",
                "url": "https://www.last.fm/music/Yuji+Toriyama/_/Korean+Dress+(Part+2)",
                "name": "Korean Dress (Part 2)",
                "image": [
                    {"size": "small", "#text": "..."},
                    {"size": "medium", "#text": "..."},
                    {"size": "large", "#text": "..."},
                    {"size": "extralarge", "#text": "..."}
                ],
                "streamable": {"fulltrack": "0", "#text": "0"}
            }
        ],
        "@attr": {
            "user": "boogerss",
            "totalPages": "87",
            "page": "1",
            "total": "259",
            "perPage": "3"
        }
    }
}
"""
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
        love_md5 varchar(32) not null primary key
        , username text not null
        , json_data jsonb not null
        , loved_at_ts_utc timestamp with time zone not null
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
    """create index {schema}_{table}_username_idx on {schema}.{table} (username)""",
    """create index {schema}_{table}_love_at_idx on {schema}.{table} (loved_at_ts_utc)""",
]


def get_loves_by_page(
    username: str,
    api_key: str = None,
    page_size: int = 100,
    page_limit: int = None,
) -> list:
    """Get recent tracks for a user in a given period."""
    params = {
        "method": "user.getLovedTracks",
        "user": username,
        "api_key": api_key or os.environ["LASTFM_API_KEY"],
        "format": "json",
        "limit": page_size,
    }

    def get_page(page: int) -> dict:
        return utils_.get_with_retries(
            "https://ws.audioscrobbler.com/2.0/", params=dict(page=page, **params)
        ).json()

    pages = [get_page(1)]
    total_loves = int(pages[0]["lovedtracks"]["@attr"]["total"])
    total_pages = int(pages[0]["lovedtracks"]["@attr"]["totalPages"])
    click.echo(f"Found {total_loves} loves in {total_pages} pages for user {username}")

    # exit early if there are no listens
    if total_loves == 0:
        click.echo('User "{}" has no loves.'.format(username))
        return []

    # get all pages if any more than one
    pages_to_get = min(page_limit or total_pages, total_pages)
    click.echo(
        f"Will query up to {pages_to_get} pages ({page_size * pages_to_get} loves)"
    )

    if pages_to_get > 1:
        for page in tqdm(range(2, pages_to_get + 1)):
            pages.append(get_page(page))

    # flatten the list of pages
    res = [item for page in pages for item in page["lovedtracks"]["track"]]
    click.echo(f"Found {len(res)} loves in total")
    return res


def love_hash(username: str, data: dict) -> str:
    """Get the hash of a love using only immutable fields."""
    immutable = [username, data["artist"]["name"], data["date"]["uts"], data["name"]]
    return hashlib.md5(json.dumps(immutable).encode("utf-8")).hexdigest()


def insert(conn, schema: str, table: str, username: str, listen_data: dict):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            insert into {schema}.{table} (love_md5, username, json_data, loved_at_ts_utc)
            values (%(love_md5)s, %(username)s, %(json_data)s, %(loved_at_ts_utc)s)
            on conflict (love_md5) do update
            set username = excluded.username
              , json_data = excluded.json_data
              , loved_at_ts_utc = excluded.loved_at_ts_utc
            """,
            dict(
                love_md5=love_hash(username, listen_data),
                username=username,
                json_data=json.dumps(listen_data),
                loved_at_ts_utc=utils_.utcfromunixtime(listen_data["date"]["uts"]),
            ),
        )


def run_ingest(
    username: str,
    table: str,
    schema: str,
    page_limit: int = None,
):
    """Ingest data from last.fm."""
    if page_limit is not None and page_limit < 1:
        raise ValueError("page_limit must be >= 1")

    click.echo(f"Getting {username} loves...")
    data = get_loves_by_page(username, page_limit=page_limit)

    if not data:
        click.echo("No loves found")
        return

    with utils_.pg_connect() as conn:
        click.echo(f"""Inserting {len(data)} loves into {schema}.{table}""")
        for row in tqdm(data):
            insert(conn, schema, table, username=username, listen_data=row)
        conn.commit()


@click.command()
@click.argument("username")
@click.option("--table", required=True)
@click.option("--schema", required=True)
@click.option(
    "--page-limit",
    type=int,
    default=None,
    help="Limit the number of pages back to query. Defaults to all pages.",
)
@click.option(
    "--create", is_flag=True, help="Option to teardown and recreate the table"
)
def main(username, table, schema, page_limit, create):
    """Query the Last.FM loved tracks api and store it in the db.

    This script is meant to be run periodically to update the db with new loves. It uses
    a md5 hash of the (user, artist name, track name, timestamp) to uniquely
    identify a love. event. A postgres on conflict clause is used to update the love
    if it already exists and ensure uniqueness."""
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    run_ingest(username=username, table=table, schema=schema, page_limit=page_limit)


if __name__ == "__main__":
    main()
