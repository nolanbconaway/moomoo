from unittest import mock
from uuid import uuid1

from click.testing import CliRunner
from moomoo_ingest import collect_listenbrainz_feedback, utils_
from moomoo_ingest.db import ListenBrainzUserFeedback
from pylistenbrainz.errors import ListenBrainzAPIException


def get_mock_lb_http(response) -> mock.Mock:
    """Make patched objects for the whole module."""

    def side_effect(*args, **kwargs):
        yield response

    return mock.patch(
        "moomoo_ingest.collect_listenbrainz_feedback.ListenBrainz._get",
        mock.Mock(side_effect=side_effect()),
    )


def test_cli_main__not_table_exists_error():
    runner = CliRunner()
    fake_users_json = dict()  # data doesn't matter, we should not get this far
    with get_mock_lb_http(fake_users_json):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE_NAME"])

    assert result.exit_code != 0
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


def test_cli_main__no_data():
    """Test the main function with empty data in the db and api."""
    ListenBrainzUserFeedback.create()

    # no db loves, no api loves. do nothing
    with get_mock_lb_http(dict(feedback=[])):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])

    assert result.exit_code == 0
    assert "No loves found via api." in result.output
    assert len(ListenBrainzUserFeedback.select_star()) == 0


def test_cli_main__valid_data():
    """Test the main function with empty data in the db but some in the api."""
    ListenBrainzUserFeedback.create()

    # start with a fresh db
    mbid = uuid1()
    fake_response = dict(
        feedback=[dict(user_id="FAKE", score=1, recording_mbid=mbid.hex, created=0)]
    )
    with get_mock_lb_http(fake_response):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])

    assert result.exit_code == 0
    assert "Latest love timestamp in the db: None" in result.output
    assert "Successfully got data for FAKE" in result.output
    assert "Inserting 1 record(s)." in result.output

    res = ListenBrainzUserFeedback.select_star()
    assert len(res) == 1
    assert res[0]["username"] == "FAKE"
    assert res[0]["score"] == 1
    assert res[0]["recording_mbid"] == mbid
    assert res[0]["feedback_at"] == utils_.utcfromunixtime(0)

    # re-running should skip the insert step
    with get_mock_lb_http(fake_response):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])

    assert "No new loves found." in result.output


# last api after last db
def test_cli_main__missing_db_warn():
    """Test the warning for potentially missing data in the db."""
    ListenBrainzUserFeedback.create()

    # make a fake db row at time 10
    ListenBrainzUserFeedback(
        feedback_md5="FAKE",
        username="FAKE",
        score=1,
        recording_mbid=uuid1(),
        feedback_at=utils_.utcfromunixtime(10),
    ).insert()

    # make the last api response at time 20
    fake_response = dict(
        feedback=[dict(user_id="FAKE", score=1, recording_mbid=uuid1().hex, created=20)]
    )
    with get_mock_lb_http(fake_response):
        runner = CliRunner()
        result = runner.invoke(collect_listenbrainz_feedback.main, ["FAKE"])

    assert result.exit_code == 0
    assert "Successfully got data for FAKE" in result.output
    assert "Inserting 1 record(s)." in result.output
    assert "WARN: Last love timestamp in the db" in result.output

    last_db_ts = utils_.utcfromunixtime(10)
    first_api_ts = utils_.utcfromunixtime(20)
    message = (
        f"WARN: Last love timestamp in the db ({last_db_ts}) is before the "
        + f"earliest api love timestamp ({first_api_ts}). Potentially some "
        + "loves are missing from the db."
    )

    assert message in result.output
