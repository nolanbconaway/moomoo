import datetime
from unittest import mock

from click.testing import CliRunner
from liblistenbrainz.errors import ListenBrainzAPIException

from moomoo_ingest import collect_similar_user_activity
from moomoo_ingest.db import ListenBrainzSimilarUserActivity


def get_mock_lb_http(similar_users, activity) -> mock.Mock:
    """Make patched objects for the whole module."""

    def side_effect(*args, **kwargs):
        yield similar_users
        if isinstance(activity, list):
            yield from activity
        else:
            while True:
                yield activity

    return mock.patch("liblistenbrainz.ListenBrainz._get", mock.Mock(side_effect=side_effect()))


def test_last_ingest_ts():
    ListenBrainzSimilarUserActivity.create()

    # no data
    assert collect_similar_user_activity.last_ingest_ts("FAKE_NAME") is None

    # with data
    ts = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    record = ListenBrainzSimilarUserActivity(
        payload_id="abc123",
        from_username="FAKE_NAME",
        to_username="FAKE_NAME_2",
        user_similarity=0.5,
        entity="artists",
        time_range="all_time",
        json_data={},
        insert_ts_utc=ts,
    )
    record.insert()
    assert collect_similar_user_activity.last_ingest_ts("FAKE_NAME") == ts


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
    assert "Inserting 9 records." in result.output.splitlines()
    assert "Done." in result.output.splitlines()

    res = ListenBrainzSimilarUserActivity.select_star()
    assert len(res) == (
        len(collect_similar_user_activity.ENTITIES) * len(collect_similar_user_activity.TIME_RANGES)
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


def test_cli_main__skip_ts():
    """Test the main function with skip timestamp."""
    ListenBrainzSimilarUserActivity.create()

    # add data
    ts = datetime.datetime.now(tz=datetime.timezone.utc)
    record = ListenBrainzSimilarUserActivity(
        payload_id="abc123",
        from_username="FAKE_NAME",
        to_username="FAKE_NAME_2",
        user_similarity=0.5,
        entity="artists",
        time_range="all_time",
        json_data={},
        insert_ts_utc=ts,
    )
    record.insert()

    runner = CliRunner()
    result = runner.invoke(
        collect_similar_user_activity.main, ["FAKE_NAME", "--skip-timeout-seconds=600"]
    )
    assert result.exit_code == 0
    assert f"Last ingest for FAKE_NAME at {ts} is newer than cutoff" in result.output


def test_cli_main__exception_handling():
    """Test the main function with exception handling."""
    ListenBrainzSimilarUserActivity.create()

    fake_users_json = dict(
        payload=[dict(user_name=f"FAKE_USER_{i}", similarity=0.5) for i in range(5)]
    )

    # will need raise each exception 3x to account for retries.
    ok_activity = dict(payload=dict(fake="yes"))
    error_204 = ListenBrainzAPIException(status_code=204, message="FAKE")
    error_500 = ListenBrainzAPIException(status_code=500, message="FAKE")

    # # OK with one error
    fake_activity = [error_500] * 9 * 3 + [ok_activity] * 1000
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code == 0

    # fail if > 10 nonhandled status code
    fake_activity = [error_500] * 11 * 3 + [ok_activity] * 1000
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ListenBrainzAPIException)

    # OK with 204
    fake_activity = [error_204] * 11 * 3 + [ok_activity] * 1000
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(collect_similar_user_activity.main, ["FAKE_NAME"])
    assert result.exit_code == 0
    assert result.output.splitlines()[-1] == "Done."
