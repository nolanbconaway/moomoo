from unittest import mock

from click.testing import CliRunner
from pylistenbrainz.errors import ListenBrainzAPIException

from moomoo import utils_
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


def create_table(schema, table):
    utils_.create_table(
        schema=schema, table=table, ddl=collect_similar_user_activity.DDL
    )


def test_cli_main__valid_data():
    fake_users_json = dict(payload=[dict(user_name="FAKE_NAME_2", similarity=0.5)])
    fake_activity = dict(payload=dict(fake="yes"))
    create_table(schema="test", table="fake")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=fake", "--schema=test"],
        )
    assert result.exit_code == 0
    assert "Inserting" in result.output

    res = utils_.execute_sql_fetchall("select * from test.fake")
    assert len(res) == (
        len(collect_similar_user_activity.ENTITIES)
        * len(collect_similar_user_activity.TIME_RANGES)
    )
    assert res[0]["from_username"] == "FAKE_NAME"
    assert res[0]["to_username"] == "FAKE_NAME_2"
    assert res[0]["user_similarity"] == 0.5
    assert res[0]["json_data"] == fake_activity["payload"]


def test_cli_main__no_users():
    fake_users_json = dict(payload=[])
    fake_activity = Exception("Should not be called")
    create_table(schema="test", table="fake")

    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=fake", "--schema=test"],
        )
    assert result.exit_code == 0
    assert "No records to insert" in result.output


def test_cli_main__exception_handling():
    # fail on nonhandled status code
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=500, message="FAKE")
    create_table(schema="test", table="fake")

    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=fake", "--schema=test"],
        )
    assert result.exit_code != 0
    assert isinstance(result.exception, ListenBrainzAPIException)

    # OK with 204
    fake_users_json = dict(payload=[dict(user_name="FAKE_USER", similarity=0.5)])
    fake_activity = ListenBrainzAPIException(status_code=204, message="FAKE")
    with get_mock_lb_http(fake_users_json, fake_activity):
        runner = CliRunner()
        result = runner.invoke(
            collect_similar_user_activity.main,
            ["FAKE_NAME", "--table=fake", "--schema=test"],
        )
    assert result.exit_code == 0
    assert "No records to insert" in result.output
