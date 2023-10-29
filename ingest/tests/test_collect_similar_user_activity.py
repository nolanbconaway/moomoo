from unittest import mock

from click.testing import CliRunner
from pylistenbrainz.errors import ListenBrainzAPIException

from moomoo_ingest.db import ListenBrainzSimilarUserActivity
from moomoo_ingest import collect_similar_user_activity


def get_mock_lb_http(similar_users, activity) -> mock.Mock:
    """Make patched objects for the whole module."""

    def side_effect(*args, **kwargs):
        yield similar_users
        if isinstance(activity, list):
            for a in activity:
                yield a
        else:
            while True:
                yield activity

    return mock.patch(
        "moomoo_ingest.collect_similar_user_activity.ListenBrainz._get",
        mock.Mock(side_effect=side_effect()),
    )


def test_cli_main__not_table_exists_error():
    runner = CliRunner()

    fake_users_json = dict(payload=[dict(user_name="FAKE_NAME_2", similarity=0.5)])
    fake_activity = dict(payload=dict(fake="yes"))
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])

    assert result.exit_code != 0
    assert "psycopg.errors.UndefinedTable" in str(result.exception)


def test_cli_main__valid_data():
    """Test the main function with valid data."""
    ListenBrainzSimilarUserActivity.create()

    fake_users_json = dict(payload=[dict(user_name="FAKE_NAME_2", similarity=0.5)])
    fake_activity = dict(payload=dict(fake="yes"))
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code == 0
    assert "Successfully got data for FAKE_NAME_2" in result.output
    assert "Inserting" in result.output

    res = ListenBrainzSimilarUserActivity.select_star()
    assert len(res) == (
        len(collect_similar_user_activity.ENTITIES)
        * len(collect_similar_user_activity.TIME_RANGES)
    )
    assert res[0]["from_username"] == "FAKE_NAME"
    assert res[0]["to_username"] == "FAKE_NAME_2"
    assert res[0]["user_similarity"] == 0.5
    assert res[0]["json_data"] == fake_activity["payload"]


def test_cli_main__no_similar_users():
    """Test the main function with no similar users."""
    ListenBrainzSimilarUserActivity.create()

    fake_users_json = dict(payload=[])
    fake_activity = Exception("Should not be called")

    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code == 0
    assert "No records to insert" in result.output


def test_cli_main__exception_handling():
    """Test the main function with exception handling."""
    ListenBrainzSimilarUserActivity.create()

    # fail on nonhandled status code
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=500, message="FAKE")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ListenBrainzAPIException)

    # OK with 204
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=204, message="FAKE")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code == 0
    assert "No records to insert" in result.output
