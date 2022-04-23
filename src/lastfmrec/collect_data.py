import argparse
import json
import os
import time
from itertools import product

import psycopg2
import requests

POSTGRES_DSN = os.environ["POSTGRES_DSN"]
USERNAME = os.environ["LASTFM_USERNAME"]
API_KEY = os.environ["LASTFM_API_KEY"]
KINDS = ("artists", "tracks", "albums")
PERIODS = ("overall", "7day", "1month", "3month", "6month", "12month")


def get_with_retries(
    *args, retries: int = 3, delay: float = 3, timeout_: float = 1, **kwargs
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


def get_top(kind: str, period: str = "7day", limit: int = 1000) -> list:
    """Get the top artists, tracks, or albums for a user.

    Qeures the API to determine the page count, and then does a query for each page.
    Appends each page to a list of results.
    """
    assert kind in KINDS
    assert period in PERIODS
    params = {
        "method": "user.gettop{}".format(kind),
        "user": USERNAME,
        "api_key": API_KEY,
        "format": "json",
        "period": period,
        "limit": limit,
    }

    def get_page(page: int) -> dict:
        return get_with_retries(
            "https://ws.audioscrobbler.com/2.0/", params=dict(page=page, **params)
        ).json()

    pages = [get_page(1)]
    total_pages = int(pages[0]["top" + kind]["@attr"]["totalPages"])

    for page in range(2, total_pages + 1):
        pages.append(get_page(page))

    # flatten the list of pages
    res = []
    for page in pages:
        res += page["top" + kind][kind[:-1]]

    return res


def insert(conn, **data):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into lastfm (kind, period, json_data)
            values (%(kind)s, %(period)s, %(json_data)s)
            """,
            data,
        )
        conn.commit()


def main(queries: list, progress: bool = False):

    with psycopg2.connect(POSTGRES_DSN) as conn:
        for query in queries:
            if progress:
                print("Querying {}".format(query))
            res = get_top(**query)
            insert(conn, **query, json_data=json.dumps(res))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--period", choices=PERIODS, default=None)
    parser.add_argument("--kind", choices=KINDS, default=None)
    args = parser.parse_args()

    queries = [
        dict(kind=i, period=j)
        for i, j in product(
            [args.kind] if args.kind is not None else KINDS,
            [args.period] if args.period is not None else PERIODS,
        )
    ]

    main(queries=queries, progress=not args.no_progress)
