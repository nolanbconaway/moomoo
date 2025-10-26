"""Query the ListenBrainz user statistics api and store similar user top activity.

Gets similar users for a given user, then gets the top activity for each of those users
in each of the entities (artists, releases, recordings) and time ranges (month, year,
all_time). Stores all combinations of these in the db. This takes serveral minutes to
run given the number of HTTP requests.


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
        return client._get(endpoint, params={"range": time_range, "count": count})["payload"]
    except ListenBrainzAPIException as e:
        if e.status_code == 204:
            return []  # no data in range
        raise e


@click.command(help=__doc__)
@click.argument("username")
def main(username: str):
    """Run the main CLI."""
    similar_users = get_similar_users(username)
    records = []
    exceptions = []
    for user, entity, time_range in product(similar_users, ENTITIES, TIME_RANGES):
        try:
            data = get_user_top_activity(
                username=user["user_name"], entity=entity, time_range=time_range
            )
        except ListenBrainzAPIException as e:
            click.echo(
                f"Failed to get top activity for {user['user_name']} "
                f"in the {entity} entity and {time_range} range: {e}",
                err=True
            )
            exceptions.append(e)
            continue

        if len(exceptions) > 10:
            click.echo("Too many failures, exiting. Raising last exception.")
            raise exceptions[-1]

        records.append(
            {
                "payload_id": hashlib.md5(
                    json.dumps([username, user["user_name"], entity, time_range]).encode()
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
            ListenBrainzSimilarUserActivity(**row, insert_ts_utc=utils_.utcnow()).upsert(
                session=session
            )

        click.echo("Insert complete.")
    click.echo("Done.")


if __name__ == "__main__":
    main()
