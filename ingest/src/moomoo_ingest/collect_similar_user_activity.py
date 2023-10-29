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

from . import utils_
from .db import ListenBrainzSimilarUserActivity, get_session

ENTITIES = ("artists", "releases", "recordings")
TIME_RANGES = ("month", "year", "all_time")

LB_RETRY = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ListenBrainzAPIException),
    reraise=True,
)


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


@click.command()
@click.argument("username")
def main(username: str):
    """Get the top releases for a user's similar users.

    Ranks the releases by the number of listens and the similarity score of the similar
    user. Returns a dict of {mbid: score} pairs, in descending order of score.
    """
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
                "json_data": data,
            }
        )

    if not records:
        click.echo("No records to insert.")
        sys.exit(0)

    click.echo(f"Inserting {len(records)} records.")
    with get_session() as session:
        click.echo(f"Deleting all records for {username}.")
        deleted = (
            session.query(ListenBrainzSimilarUserActivity)
            .filter(ListenBrainzSimilarUserActivity.from_username == username)
            .delete()
        )
        click.echo(f"Deleted {deleted} records for {username}.")

        for row in records:
            ListenBrainzSimilarUserActivity(
                **row, insert_ts_utc=utils_.utcnow()
            ).upsert(session=session)

    click.echo("Done.")


if __name__ == "__main__":
    main()
