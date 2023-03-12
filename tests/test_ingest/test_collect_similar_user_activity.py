from unittest import mock

import pytest
from click.testing import CliRunner
from pylistenbrainz.errors import ListenBrainzAPIException

from moomoo.ingest import collect_similar_user_activity


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
        "moomoo.ingest.collect_similar_user_activity.ListenBrainz._get",
        mock.Mock(side_effect=side_effect()),
    )


@pytest.fixture(autouse=True)
def mock_insert(monkeypatch):
    monkeypatch.setattr(
        collect_similar_user_activity, "insert", lambda *args, **kwargs: ...
    )


def test_cli_main__valid_data():
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = dict(payload=dict(fake="yes"))
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=FAKE", "--schema=FAKE"],
        )
    assert result.exit_code == 0
    assert "Inserting" in result.output


def test_cli_main__no_users():
    fake_users_json = dict(payload=[])
    fake_activity = Exception("Should not be called")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=FAKE", "--schema=FAKE"],
        )
    assert result.exit_code == 0
    assert "No records to insert" in result.output


def test_cli_main__exception_handling():
    # fail on nonhandled status code
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=500, message="FAKE")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=FAKE", "--schema=FAKE"],
        )
    assert result.exit_code != 0

    # OK with 204
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=204, message="FAKE")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=FAKE", "--schema=FAKE"],
        )
    assert result.exit_code == 0
    assert "No records to insert" in result.output
