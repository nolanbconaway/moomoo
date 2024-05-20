"""Query the ListenBrainz recording api for loved tracks.

This script is meant to be run periodically to update the db with recent loves. It uses
an md5 hash of the (user, score, recording_mbid) to uniquely identify a loved track
event; with duplicates being upserted.

API Docs: https://listenbrainz.readthedocs.io/en/latest/users/api/recordings.html#get--1-feedback-user-(user_name)-get-feedback
"""

import datetime
import sys
from dataclasses import dataclass
from uuid import UUID

import click
from pylistenbrainz import ListenBrainz
from pylistenbrainz.errors import ListenBrainzAPIException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from . import utils_
from .db import ListenBrainzUserFeedback, get_session


@dataclass
class UserFeedback:
    """A loved recording."""

    username: str
    score: int
    recording_mbid: UUID
    feedback_at: datetime.datetime

    @property
    def feedback_md5(self) -> str:
        """Return a unique identifier for this feedback."""
        return utils_.md5(self.username, str(self.score), str(self.recording_mbid))

    def to_dict(self) -> dict:
        """Return a dict representation of this object."""
        return {
            "feedback_md5": self.feedback_md5,
            "username": self.username,
            "score": self.score,
            "recording_mbid": self.recording_mbid,
            "feedback_at": self.feedback_at,
        }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ListenBrainzAPIException),
    reraise=True,
)
def get_most_recent_feedback(username: str) -> list[UserFeedback]:
    """Get loved recordings for a user.

    Will return the last 100 loved recordings for a user, in reverse chronological
    order.
    """
    client = ListenBrainz()
    click.echo(f"Getting user feedback for for {username}.")

    url = f"1/feedback/user/{username}/get-feedback"
    params = {
        "count": 100,
        "score": 1,  # loves only
        "offset": 0,  # this offsets from the BEGINNING of the list (i.e., most recent)
        "metadata": False,
    }

    # expect a response like:
    # {
    #     ...
    #     "feedback": [
    #         {
    #             "created": unixts, "recording_mbid": "..", "score": 1, "user_id": ".."
    #         },
    #         ...
    #     ],
    # }

    res = [
        UserFeedback(
            username=i["user_id"],
            score=i["score"],
            recording_mbid=UUID(i["recording_mbid"]),
            feedback_at=utils_.utcfromunixtime(i["created"]),
        )
        for i in client._get(url, params=params)["feedback"]
    ]

    click.echo(f"Successfully got data for {username} ({len(res)} records).")
    return res


@click.command(help=__doc__)
@click.argument("username")
def main(username: str):
    """Run the main CLI."""
    last_db_ts = ListenBrainzUserFeedback.last_love_for_user(username)
    loves = get_most_recent_feedback(username)

    if not loves:
        click.echo("No loves found via api.")
        sys.exit(0)

    first_api_ts = loves[-1].feedback_at
    last_api_ts = loves[0].feedback_at
    click.echo(f"Earliest love timestamp from the api: {first_api_ts}.")
    click.echo(f"Latest love timestamp from the api: {last_api_ts}.")
    click.echo(f"Latest love timestamp in the db: {last_db_ts}.")

    # do a series of tests against the db and api to make sure we're not missing any
    # loves.
    if last_db_ts:
        # exit early if the latest api is the same as the latest db
        if last_db_ts == last_api_ts:
            click.echo("No new loves found.")
            sys.exit(0)

        # warn user if no overlap between api and db. in this case there could be
        # loves in the db that are not in the api list.
        if last_db_ts < first_api_ts:
            click.echo(
                (
                    f"WARN: Last love timestamp in the db ({last_db_ts}) is before the "
                    + f"earliest api love timestamp ({first_api_ts}). Potentially some "
                    + "loves are missing from the db."
                ),
                err=True,
            )

    # filter to new loves only. no need to log, as there should always be new loves
    # because of the last_db_ts check above.
    if last_db_ts:
        loves = [love for love in loves if love.feedback_at > last_db_ts]

    click.echo(f"Inserting {len(loves)} record(s).")
    with get_session() as session:
        for row in loves:
            ListenBrainzUserFeedback(**row.to_dict(), insert_ts_utc=utils_.utcnow()).upsert(
                session=session
            )

        click.echo("Insert complete.")
    click.echo("Done.")


if __name__ == "__main__":
    main()
