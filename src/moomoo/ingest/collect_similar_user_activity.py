"""Query the ListenBrainz user statistics api and store similar user top activity.

This script is meant to be run periodically and upload whatever data is available to the
database. It will not overwrite existing data. So DBT will be needed to merge latest,
over time, etc.
"""
import hashlib
import json
import sys
from itertools import product
from typing import Union

import click
from pylistenbrainz import ListenBrainz
from pylistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .. import utils_

ENTITIES = ("artists", "releases", "recordings")
TIME_RANGES = ("month", "year", "all_time")

LB_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ListenBrainzAPIException),
    reraise=True,
)

DDL = [
    """
    create table {schema}.{table} (
        payload_id varchar(32) not null primary key
        , from_username text not null
        , to_username text not null
        , entity varchar not null
        , time_range varchar not null
        , user_similarity float not null
        , json_data jsonb not null
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
    "create index {schema}_{table}_from_idx on {schema}.{table} (from_username)",
    "create index {schema}_{table}_to_idx on {schema}.{table} (to_username)",
    "create index {schema}_{table}_entity_idx on {schema}.{table} (entity)",
    "create index {schema}_{table}_time_range_idx on {schema}.{table} (time_range)",
    "create index {schema}_{table}_at_idx on {schema}.{table} (insert_ts_utc)",
]


@LB_RETRY
def get_similar_users(username: str) -> list[dict[str, Union[str, float]]]:
    """Get similar users for a user.

    Returns a list of dicts with the following keys:
        - user_name (str) - the username of the similar user
        - similarity (float) - the similarity score between the two users, from 0-1.
    """
    client = ListenBrainz()
    click.echo(f"Getting similar users for {username}.")
    return client._get(f"/1/user/{username}/similar-users")["payload"]


@LB_RETRY
def get_user_top_activity(
    username: str, entity: str, time_range: str = "all_time", count: int = 100
) -> list[dict[str, Union[str, float]]]:
    """Get the top activity for a user/entity."""
    if entity not in ENTITIES:
        raise ValueError(f"Invalid entity: {entity}.")
    if time_range not in TIME_RANGES:
        raise ValueError(f"Invalid time range: {range}.")
    if count < 1 or count > 100:
        raise ValueError(f"Invalid count: {count}.")

    client = ListenBrainz()
    endpoint = f"/1/stats/user/{username}/{entity}"
    click.echo(f"Getting top {entity} for {username} in the {time_range} range.")
    try:
        return client._get(endpoint, params={"range": time_range, "count": count})[
            "payload"
        ]
    except ListenBrainzAPIException as e:
        if e.status_code == 204:
            return []  # no data in range
        raise e


def insert(conn, schema: str, table: str, data: list[dict], username: str):
    sql = f"""
        insert into {schema}.{table} (
            payload_id
            , from_username
            , to_username
            , user_similarity
            , entity
            , time_range
            , json_data
        ) 
        values (
            %(payload_id)s
            , %(from_username)s
            , %(to_username)s
            , %(user_similarity)s
            , %(entity)s
            , %(time_range)s
            , %(json_data)s
        )
        on conflict (payload_id) do update set
            from_username = excluded.from_username
            , to_username = excluded.to_username
            , user_similarity = excluded.user_similarity
            , entity = excluded.entity
            , time_range = excluded.time_range
            , json_data = excluded.json_data
            , insert_ts_utc = current_timestamp
    """

    with conn.cursor() as cur:
        cur.execute(
            f"delete from {schema}.{table} where from_username = %s", (username,)
        )
        cur.executemany(sql, data)


@click.command()
@click.argument("username")
@click.option("--table", required=True)
@click.option("--schema", required=True)
@click.option(
    "--create", is_flag=True, help="Option to teardown and recreate the table"
)
def main(
    username: str,
    table: str,
    schema: str,
    create: bool,
):
    """Get the top releases for a user's similar users.

    Ranks the releases by the number of listens and the similarity score of the similar
    user. Returns a dict of {mbid: score} pairs, in descending order of score.
    """
    if create:
        utils_.create_table(schema, table, DDL)
    elif not utils_.check_table_exists(schema=schema, table=table):
        click.echo(f"Table {schema}.{table} does not exist. Use --create to create it.")
        sys.exit(1)

    similar_users = get_similar_users(username)
    records = []
    for user, entity, time_range in product(similar_users, ENTITIES, TIME_RANGES):
        data = get_user_top_activity(
            username=user["user_name"], entity=entity, time_range=time_range
        )

        if not data:
            click.echo(f"No data for {user['user_name']} in the {time_range} range.")
            continue
        else:
            click.echo(f"Successfully got data for {user['user_name']}.")

        records.append(
            {
                "payload_id": hashlib.md5(
                    json.dumps(
                        [username, user["user_name"], entity, time_range]
                    ).encode()
                ).hexdigest(),
                "from_username": username,
                "to_username": user["user_name"],
                "entity": entity,
                "time_range": time_range,
                "user_similarity": user["similarity"],
                "json_data": json.dumps(data),
            }
        )

    if not records:
        click.echo("No records to insert.")
        sys.exit(0)

    click.echo(f"Inserting {len(records)} records into {schema}.{table}.")
    with utils_.pg_connect() as conn:
        insert(
            conn=conn,
            schema=schema,
            table=table,
            data=records,
            username=username,
        )

    click.echo("Done.")


if __name__ == "__main__":
    main()
