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
from liblistenbrainz.errors import ListenBrainzAPIException
from requests.exceptions import ConnectionError as RequestsConnectionError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from . import utils_
from .db import ListenBrainzUserFeedback, get_session

PAGE_SIZE = 100


# global ListenBrainz client, rate limiting is handled internally
client = utils_.get_listenbrainz_client()


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
    retry=retry_if_exception_type(
        (ListenBrainzAPIException, RequestsConnectionError, ConnectionError)
    ),
    reraise=True,
)
def get_total_feedback_count(username: str) -> int:
    """Get the total number of feedback records for a user."""
    click.echo(f"Getting total feedback count for {username}.")
    url = f"1/feedback/user/{username}/get-feedback"
    params = {
        "count": 0,
        "score": 1,  # loves only
        "offset": 0,
        "metadata": False,
    }
    res = client._get(url, params=params)
    res = int(res["total_count"])
    click.echo(f"Successfully got count for {username} ({res} records).")
    return res


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(
        (ListenBrainzAPIException, RequestsConnectionError, ConnectionError)
    ),
    reraise=True,
)
def get_feedback_page(username: str, page_num: int = 0) -> list[UserFeedback]:
    """Get a page of feedback for a user.

    Pages are 100 records long, and in reverse chronological order (most recent first).
    """
    click.echo(f"Getting user feedback for for {username}/page {page_num}.")

    url = f"1/feedback/user/{username}/get-feedback"
    params = {
        "count": PAGE_SIZE,
        "score": 1,  # loves only
        "offset": page_num * PAGE_SIZE,
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
    # figure out how many pages we need to get.
    feedback_count = get_total_feedback_count(username)
    num_pages = feedback_count // PAGE_SIZE + 1

    # get the feedback from http.
    loves: list[UserFeedback] = []
    click.echo(f"Getting {num_pages} page(s) of feedback for {username}.")
    for page in list(range(num_pages))[::-1]:
        loves += get_feedback_page(username, page)

    # if resync, delete all records for this user
    click.echo(f"Deleting {feedback_count} record(s) for {username}.")
    with get_session() as session:
        session.query(ListenBrainzUserFeedback).filter_by(username=username).delete()
        session.commit()

    if not loves:
        click.echo("No loves found via api. Nothing to do.")
        sys.exit(0)

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
